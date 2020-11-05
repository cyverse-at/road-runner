package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/cyverse-de/road-runner/fs"
	"github.com/cyverse-de/road-runner/singularity"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/cyverse-de/messaging.v8"
	"gopkg.in/cyverse-de/model.v5"
)

// logrusProxyWriter will prevent
// "Error while reading from Writer: bufio.Scanner: token too long" errors
// if a singularity command generates a lot of output
// (from pulling many input containers at once, for example)
// and Logrus attempts to log all of that output in one log line.
type logrusProxyWriter struct {
	entry *logrus.Entry
}

func (w *logrusProxyWriter) Write(b []byte) (int, error) {
	return fmt.Fprintf(w.entry.Writer(), string(b))
}

var logWriter = &logrusProxyWriter{
	entry: log,
}

// JobRunner provides the functionality needed to run jobs.
type JobRunner struct {
	client      JobUpdatePublisher
	exit        chan messaging.StatusCode
	job         *model.Job
	status      messaging.StatusCode
	cfg         *viper.Viper
	logsDir     string
	volumeDir   string
	workingDir  string
	projectName string
	tmpDir      string
	composer    *singularity.JobCompose
}

// NewJobRunner creates a new JobRunner
func NewJobRunner(client JobUpdatePublisher, composer *singularity.JobCompose, job *model.Job, cfg *viper.Viper, exit chan messaging.StatusCode) (*JobRunner, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	runner := &JobRunner{
		client:     client,
		exit:       exit,
		job:        job,
		cfg:        cfg,
		status:     messaging.Success,
		workingDir: cwd,
		volumeDir:  path.Join(cwd, singularity.VOLUMEDIR),
		logsDir:    path.Join(cwd, singularity.VOLUMEDIR, "logs"),
		tmpDir:     path.Join(cwd, singularity.TMPDIR),
		composer:   composer,
	}
	return runner, nil
}

// Init will initialize the state for a JobRunner. The volumeDir and logsDir
// will get created.
func (r *JobRunner) Init() error {
	err := os.MkdirAll(r.logsDir, 0755)
	if err != nil {
		return err
	}

	err = os.MkdirAll(r.tmpDir, 0755)
	if err != nil {
		return err
	}

	// Set world-write perms on volumeDir, so non-root users can create job outputs.
	err = os.Chmod(r.volumeDir, 0777)
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Set world-write perms on tmpDir, so non-root users can create temp outputs.
	err = os.Chmod(r.tmpDir, 0777)
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Copy singularity.yml file to the log dir for debugging purposes.
	singymlPath := "singularity.yml"
	err = fs.CopyFile(fs.FS, singymlPath, path.Join(r.logsDir, singymlPath))
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Copy upload exclude list to the log dir for debugging purposes.
	err = fs.CopyFile(fs.FS, singularity.UploadExcludesFilename, path.Join(r.logsDir, singularity.UploadExcludesFilename))
	if err != nil {
		// Log error and continue.
		log.Error(err)
	}

	// Copy input path list to the log dir for debugging purposes.
	if r.job.InputPathListFile != "" {
		err = fs.CopyFile(fs.FS, r.job.InputPathListFile, path.Join(r.logsDir, r.job.InputPathListFile))
		if err != nil {
			// Log error and continue.
			log.Error(err)
		}
	}

	transferTrigger, err := os.Create(path.Join(r.logsDir, "de-transfer-trigger.log"))
	if err != nil {
		return err
	}
	defer transferTrigger.Close()
	_, err = transferTrigger.WriteString("This is only used to force HTCondor to transfer files.")
	if err != nil {
		return err
	}

	if _, err = os.Stat("iplant.cmd"); err != nil {
		if err = os.Rename("iplant.cmd", path.Join(r.logsDir, "iplant.cmd")); err != nil {
			return err
		}
	}

	return nil
}

func (r *JobRunner) getImages() map[string][]string {
	result := make(map[string][]string)

	for _, svc := range r.composer.Instances {
		result[svc.Container] = svc.AuthEnv
	}

	return result
}

// JobUpdatePublisher is the interface for types that need to publish a job
// update.
type JobUpdatePublisher interface {
	PublishJobUpdate(m *messaging.UpdateMessage) error
}

func (r *JobRunner) downloadInputs(ctx context.Context) (messaging.StatusCode, error) {
	env := os.Environ()
	singularityBin := r.cfg.GetString("singularity.path")
	if job.InputPathListFile != "" {
		return r.downloadInputStep(ctx, "download_inputs", job.InputPathListFile, singularityBin, env)
	}
	for index, input := range r.job.Inputs() {
		svcname := fmt.Sprintf("input_%d", index)
		if status, err := r.downloadInputStep(ctx, svcname, input.IRODSPath(), singularityBin, env); err != nil {
			return status, err
		}
	}

	return messaging.Success, nil
}

func (r *JobRunner) downloadInputStep(ctx context.Context, svcname, inputPath, singularityBin string, env []string) (messaging.StatusCode, error) {
	var (
		exitCode int64
	)
	running(r.client, r.job, fmt.Sprintf("Downloading %s", inputPath))
	stderr, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stderr-%s", svcname)))
	if err != nil {
		log.Error(err)
	}
	defer stderr.Close()
	stdout, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stdout-%s", svcname)))
	if err != nil {
		log.Error(err)
	}
	defer stdout.Close()

	singenv, cmdline := r.composer.GenCmdline(svcname, false)
	singenv = append(singenv, env...)
	downloadCommand := exec.CommandContext(ctx, singularityBin, cmdline...)
	downloadCommand.Env = singenv
	downloadCommand.Stdout = stdout
	downloadCommand.Stderr = stderr
	err = downloadCommand.Run()

	if err != nil {
		running(r.client, r.job, fmt.Sprintf("error downloading %s: %s", inputPath, err.Error()))
		return messaging.StatusInputFailed, errors.Wrapf(err, "failed to download %s with an exit code of %d", inputPath, exitCode)
	}
	stdout.Close()
	stderr.Close()
	running(r.client, r.job, fmt.Sprintf("finished downloading %s", inputPath))

	return messaging.Success, nil
}

func (r *JobRunner) runAllSteps(ctx context.Context) (messaging.StatusCode, error) {
	var err error

	for idx, step := range r.job.Steps {
		running(r.client, r.job,
			fmt.Sprintf(
				"Running tool container %s:%s with arguments: %s",
				step.Component.Container.Image.Name,
				step.Component.Container.Image.Tag,
				strings.Join(step.Arguments(), " "),
			),
		)

		stdout, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("step-stdout-%d", idx)))
		if err != nil {
			log.Error(err)
		}
		defer stdout.Close()

		stderr, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("step-stderr-%d", idx)))
		if err != nil {
			log.Error(err)
		}
		defer stderr.Close()

		singularityBin := r.cfg.GetString("singularity.path")
		svcname := fmt.Sprintf("step_%d", idx)

		singenv, cmdline := r.composer.GenCmdline(svcname, true)
		singenv = append(singenv, os.Environ()...)

		runCommand := exec.CommandContext(ctx, singularityBin, cmdline...)
		runCommand.Env = singenv
		runCommand.Stdout = stdout
		runCommand.Stderr = stderr
		err = runCommand.Run()

		if err != nil {
			running(r.client, r.job,
				fmt.Sprintf(
					"Error running tool container %s:%s with arguments '%s': %s",
					step.Component.Container.Image.Name,
					step.Component.Container.Image.Tag,
					strings.Join(step.Arguments(), " "),
					err.Error(),
				),
			)

			return messaging.StatusStepFailed, err
		}

		running(r.client, r.job,
			fmt.Sprintf("Tool container %s:%s with arguments '%s' finished successfully",
				step.Component.Container.Image.Name,
				step.Component.Container.Image.Tag,
				strings.Join(step.Arguments(), " "),
			),
		)
		// stdout.Close()
		// stderr.Close()
	}
	return messaging.Success, err
}

func (r *JobRunner) uploadOutputs() (messaging.StatusCode, error) {
	var err error
	singularityBin := r.cfg.GetString("singularity.path")
	stdout, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stdout-output")))
	if err != nil {
		log.Error(err)
	}
	defer stdout.Close()
	stderr, err := os.Create(path.Join(r.logsDir, fmt.Sprintf("logs-stderr-output")))
	if err != nil {
		log.Error(err)
	}
	defer stderr.Close()

	singenv, cmdline := r.composer.GenCmdline("upload_outputs", true)
	outputCommand := exec.Command(singularityBin, cmdline...)
	outputCommand.Env = singenv
	outputCommand.Stdout = stdout
	outputCommand.Stderr = stderr
	err = outputCommand.Run()

	if err != nil {
		running(r.client, r.job, fmt.Sprintf("Error uploading outputs to %s: %s", r.job.OutputDirectory(), err.Error()))
		return messaging.StatusOutputFailed, errors.Wrapf(err, "failed to upload outputs to %s", r.job.OutputDirectory())
	}

	running(r.client, r.job, fmt.Sprintf("Done uploading outputs to %s", r.job.OutputDirectory()))
	return messaging.Success, nil
}

func (r *JobRunner) pullImages() (messaging.StatusCode, error) {
	images := r.getImages()

	env := os.Environ()
	singularityBin := r.cfg.GetString("singularity.path")

	logfile, err := os.Create(path.Join(r.logsDir, "singularity-pull.log"))
	if err != nil {
		log.Error(err)
	}
	defer logfile.Close()

	for imageName, authEnv := range images {
		pullCommand := exec.Command(singularityBin, "pull", imageName)
		pullCommand.Env = append(authEnv, env...)
		pullCommand.Dir = r.workingDir
		pullCommand.Stdout = logfile
		pullCommand.Stderr = logfile

		err = pullCommand.Run()
		if err != nil {
			log.Error(err)
			return messaging.StatusDockerPullFailed, errors.Wrapf(err, "failed to pull image: %s", imageName)
		}
	}

	return messaging.Success, nil
}

// Run executes the job, and returns the exit code on the exit channel.
func Run(ctx context.Context, client JobUpdatePublisher, composer *singularity.JobCompose, job *model.Job, cfg *viper.Viper, exit chan messaging.StatusCode) {
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		host = "UNKNOWN"
	}

	runner, err := NewJobRunner(client, composer, job, cfg, exit)
	if err != nil {
		log.Error(err)
	}

	err = runner.Init()
	if err != nil {
		log.Error(err)
	}

	runner.projectName = strings.Replace(runner.job.InvocationID, "-", "", -1)

	// let everyone know the job is running
	running(runner.client, runner.job, fmt.Sprintf("Job %s is running on host %s", runner.job.InvocationID, host))

	if runner.status, err = runner.pullImages(); err != nil {
		log.Error(err)
	}

	if err = fs.WriteJobSummary(fs.FS, runner.logsDir, job); err != nil {
		log.Error(err)
	}

	if err = fs.WriteJobParameters(fs.FS, runner.logsDir, job); err != nil {
		log.Error(err)
	}

	// If pulls didn't succeed then we can't guarantee that we've got the
	// correct versions of the tools. Don't bother pulling in data in that case,
	// things are already screwed up.
	if runner.status == messaging.Success {
		if runner.status, err = runner.downloadInputs(ctx); err != nil {
			log.Error(err)
		}
	}
	// Only attempt to run the steps if the input downloads succeeded. No reason
	// to run the steps if there's no/corrupted data to operate on.
	if runner.status == messaging.Success {
		if runner.status, err = runner.runAllSteps(ctx); err != nil {
			log.Error(err)
		}
	}
	// Always attempt to transfer outputs. There might be logs that can help
	// debug issues when the job fails.
	var outputStatus messaging.StatusCode
	running(runner.client, runner.job, fmt.Sprintf("Beginning to upload outputs to %s", runner.job.OutputDirectory()))
	if outputStatus, err = runner.uploadOutputs(); err != nil {
		log.Error(err)
	}
	if outputStatus != messaging.Success {
		runner.status = outputStatus
	}
	// Always inform upstream of the job status.
	if runner.status != messaging.Success {
		fail(runner.client, runner.job, fmt.Sprintf("Job exited with a status of %d", runner.status))
	} else {
		success(runner.client, runner.job)
	}
	exit <- runner.status
}
