package singularity

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/spf13/viper"
	"gopkg.in/cyverse-de/model.v5"
)

// WORKDIR is the path to the working directory inside all of the containers
// that are run as part of a job.
const WORKDIR = "/de-app-work"

// CONFIGDIR is the path to the local configs inside the containers that are
// used to transfer files into and out of the job.
const CONFIGDIR = "/configs"

// IRODSCONFIGNAME is the basename of the irods config file
const IRODSCONFIGNAME = "irods-config"

// VOLUMEDIR is the name of the directory that is used for the working directory
// volume.
const VOLUMEDIR = "workingvolume"

// TMPDIR is the name of the directory that will be mounted into the container
// as the /tmp directory.
const TMPDIR = "tmpfiles"

const (
	// UploadExcludesFilename is the file listing porklock upload exclusions
	UploadExcludesFilename string = "porklock-upload-exclusions.txt"
)

// Instance configures a singularity service.
type Instance struct {
	Container   string            `s:"container"`
	EntryPoint  string            `s:"command"`
	Arguments   []string          `s:""`
	Environment map[string]string `s:"SINGULARITYENV_"`
	AuthEnv     []string          `s:""`
	WorkingDir  string            `s:"--pwd"`
	CapAdd      []string          `s:"--add-caps"`
	CapDrop     []string          `s:"--drop-caps"`
	Volumes     []string          `s:"--bind"`
}

// JobCompose is the top-level type for a singularity job.
type JobCompose struct {
	Instances map[string]*Instance
}

// New returns a newly instantiated *JobCompose instance.
func New() (*JobCompose, error) {
	return &JobCompose{
		Instances: make(map[string]*Instance),
	}, nil
}

// InitFromJob fills out values as appropriate for running in the DE's Condor
// Cluster.
func (j *JobCompose) InitFromJob(job *model.Job, cfg *viper.Viper, workingdir string) {
	// The host path for the working directory volume mount
	workingVolumeHostPath := path.Join(workingdir, VOLUMEDIR)
	irodsConfigPath := path.Join(workingdir, IRODSCONFIGNAME)

	porklockImage := cfg.GetString("porklock.image")
	porklockTag := cfg.GetString("porklock.tag")
	porklockImageName := fmt.Sprintf("docker://%s:%s", porklockImage, porklockTag)

	if job.InputPathListFile != "" {
		inputPathListPath := path.Join(workingdir, job.InputPathListFile)
		inputPathListMount := path.Join(CONFIGDIR, job.InputPathListFile)

		inputsSvc := NewPorklockService(
			job.InvocationID,
			workingVolumeHostPath,
			irodsConfigPath,
			porklockImageName,
			job.InputSourceListArguments(inputPathListMount),
		)
		inputsSvc.Volumes = append(
			inputsSvc.Volumes,
			strings.Join([]string{inputPathListPath, inputPathListMount, "ro"}, ":"),
		)

		j.Instances["download_inputs"] = inputsSvc
	} else {
		// Backwards compatibility for individual input downloads
		for index, input := range job.Inputs() {
			j.Instances[fmt.Sprintf("input_%d", index)] = NewPorklockService(
				job.InvocationID,
				workingVolumeHostPath,
				irodsConfigPath,
				porklockImageName,
				input.Arguments(job.Submitter, job.FileMetadata),
			)
		}
	}

	// Add the steps to the singularity instances.
	for index, step := range job.Steps {
		j.ConvertStep(&step, index, job.Submitter, job.InvocationID, workingVolumeHostPath)
	}

	// Add the final output job
	excludesPath := path.Join(workingdir, UploadExcludesFilename)
	excludesMount := path.Join(CONFIGDIR, UploadExcludesFilename)

	uploadOutputsSvc := NewPorklockService(
		job.InvocationID,
		workingVolumeHostPath,
		irodsConfigPath,
		porklockImageName,
		job.FinalOutputArguments(excludesMount),
	)
	uploadOutputsSvc.Volumes = append(
		uploadOutputsSvc.Volumes,
		strings.Join([]string{excludesPath, excludesMount, "ro"}, ":"),
	)

	j.Instances["upload_outputs"] = uploadOutputsSvc
}

// NewPorklockService generates a singularity service for porklock
func NewPorklockService(invocationID, workingVolumeHostPath, irodsConfigPath, porklockImageName string, porklockArgs []string) *Instance {
	return &Instance{
		Container: porklockImageName,
		Arguments: porklockArgs,
		Environment: map[string]string{
			"JOB_UUID": invocationID,
		},
		WorkingDir: WORKDIR,
		CapAdd:     []string{"IPC_LOCK"},
		Volumes: []string{
			strings.Join([]string{workingVolumeHostPath, WORKDIR, "rw"}, ":"),
			strings.Join([]string{irodsConfigPath, path.Join(CONFIGDIR, IRODSCONFIGNAME), "ro"}, ":"),
		},
	}
}

type authInfo struct {
	Username string
	Password string
}

func parseAuthInfo(b64 string) (*authInfo, error) {
	jsonstring, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	a := &authInfo{}
	err = json.Unmarshal(jsonstring, a)
	return a, err
}

// parseImage parses a ContainerImage to an image url and singularity environment variables for authentication
func parseImage(image *model.ContainerImage) (string, []string) {
	var imageName string
	if image.Tag != "" {
		imageName = fmt.Sprintf(
			"docker://%s:%s",
			image.Name,
			image.Tag,
		)
	} else {
		imageName = fmt.Sprintf("docker://%s", image.Name)
	}

	var environ []string
	if image.Auth != "" {
		creds, err := parseAuthInfo(image.Auth)
		if err == nil {
			environ = []string{fmt.Sprintf("SINGULARITY_DOCKER_USERNAME=%s", creds.Username),
				fmt.Sprintf("SINGULARITY_DOCKER_PASSWORD=%s", creds.Password)}
		}
	}

	return imageName, environ
}

// ConvertStep will add the job step to the JobCompose Instances
func (j *JobCompose) ConvertStep(step *model.Step, index int, user, invID, workingDirHostPath string) {

	step.Environment["IPLANT_USER"] = user
	step.Environment["IPLANT_EXECUTION_ID"] = invID

	imageName, authEnv := parseImage(&step.Component.Container.Image)

	j.Instances[fmt.Sprintf("step_%d", index)] = &Instance{
		Container:   imageName,
		Arguments:   step.Arguments(),
		Environment: step.Environment,
		AuthEnv:     authEnv,
		WorkingDir:  step.Component.Container.WorkingDirectory(),
	}

	svc := j.Instances[fmt.Sprintf("step_%d", index)]
	stepContainer := step.Component.Container

	if stepContainer.EntryPoint != "" {
		svc.EntryPoint = stepContainer.EntryPoint
	}

	// Handles volumes created by other containers.
	for _, dc := range stepContainer.VolumesFrom {
		if dc.HostPath != "" || dc.ContainerPath != "" {
			var rw string
			if dc.ReadOnly {
				rw = "ro"
			} else {
				rw = "rw"
			}

			svc.Volumes = append(svc.Volumes,
				fmt.Sprintf("%s:%s:%s", dc.HostPath, dc.ContainerPath, rw),
			)
		}
	}

	// The working directory needs to be mounted as a volume.
	svc.Volumes = append(svc.Volumes, strings.Join([]string{workingDirHostPath, stepContainer.WorkingDirectory(), "rw"}, ":"))

	// The TMPDIR needs to be mounted as a volume
	svc.Volumes = append(svc.Volumes, fmt.Sprintf("./%s:/tmp:rw", TMPDIR))

	for _, v := range stepContainer.Volumes {
		var rw string
		if v.ReadOnly {
			rw = "ro"
		} else {
			rw = "rw"
		}
		if v.HostPath == "" {
			svc.Volumes = append(svc.Volumes, fmt.Sprintf("%s:%s", v.ContainerPath, rw))
		} else {
			svc.Volumes = append(svc.Volumes, fmt.Sprintf("%s:%s:%s", v.HostPath, v.ContainerPath, rw))
		}
	}
}

// GenCmdline returns the command line arguments for running a job in singularity
func (j *JobCompose) GenCmdline(svcname string, entrypoint bool) ([]string, []string) {
	svc := j.Instances[svcname]

	var cmdline []string
	if len(svc.EntryPoint) > 0 && entrypoint {
		cmdline = append(cmdline, "exec")
	} else {
		cmdline = append(cmdline, "run")
	}

	if len(svc.WorkingDir) > 0 {
		cmdline = append(cmdline, "--pwd")
		cmdline = append(cmdline, svc.WorkingDir)
	}
	/* needs root?
	if len(svc.CapAdd) > 0 {
		cmdline = append(cmdline, "--add-caps")
		cmdline = append(cmdline, strings.Join(svc.CapAdd, ","))
	}

	if len(svc.CapDrop) > 0 {
		cmdline = append(cmdline, "--drop-caps")
		cmdline = append(cmdline, strings.Join(svc.CapDrop, ","))
	}
	*/
	if len(svc.Volumes) > 0 {
		cmdline = append(cmdline, "--bind")
		cmdline = append(cmdline, strings.Join(svc.Volumes, ","))
	}

	cmdline = append(cmdline, svc.Container)

	if len(svc.EntryPoint) > 0 && entrypoint {
		cmdline = append(cmdline, svc.EntryPoint)
	}

	cmdline = append(cmdline, svc.Arguments...)

	var environ []string
	for k, v := range svc.Environment {
		environ = append(environ, fmt.Sprintf("SINGULARITYENV_%s=%s", k, v))
	}

	return environ, cmdline
}
