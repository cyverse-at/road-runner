package dcompose

import (
	"reflect"
	"testing"

	"github.com/cyverse-de/model"

	yaml "gopkg.in/yaml.v2"
)

var testJob = &model.Job{
	ID:           "test-job-id",
	InvocationID: "test-invocation-id",
	Submitter:    "test-submitter",
	Steps: []model.Step{
		{
			Type:       "condor",
			StdinPath:  "/stdin/path",
			StdoutPath: "/stdout/path",
			StderrPath: "/stderr/path",
			LogFile:    "/logfile/path",
			Environment: map[string]string{
				"FOO": "BAR",
				"BAZ": "1",
			},
			Input: []model.StepInput{
				{
					ID:           "step-input-1",
					Multiplicity: "wut",
					Name:         "step-input-name-1",
					Property:     "step-input-property-1",
					Retain:       false,
					Type:         "step-input-type-1",
					Value:        "step-input-value-1",
				},
				{
					ID:           "step-input-2",
					Multiplicity: "wut2",
					Name:         "step-input-name-2",
					Property:     "step-input-property-2",
					Retain:       false,
					Type:         "step-input-type-2",
					Value:        "step-input-value-2",
				},
			},
			Config: model.StepConfig{
				Params: []model.StepParam{
					{
						ID:    "step-param-1",
						Name:  "step-param-name-1",
						Value: "step-param-value-1",
						Order: 0,
					},
					{
						ID:    "step-param-2",
						Name:  "step-param-name-2",
						Value: "step-param-value-2",
						Order: 1,
					},
				},
			},
			Component: model.StepComponent{
				Container: model.Container{
					ID:        "container-id-1",
					Name:      "container-name-1",
					CPUShares: 0,
					Image: model.ContainerImage{
						ID:   "container-image-1",
						Name: "container-image-name-1",
						Tag:  "container-image-tag-1",
					},
					VolumesFrom: []model.VolumesFrom{
						{
							Tag:           "tag1",
							Name:          "name1",
							HostPath:      "/host/path1",
							ContainerPath: "/container/path1",
						},
						{
							Tag:           "tag2",
							Name:          "name2",
							HostPath:      "/host/path2",
							ContainerPath: "/container/path2",
						},
					},
				},
			},
		},
	},
}

func TestJobCompose(t *testing.T) {
	expected := `version: '2'
volumes:
  test0:
    driver: local
    driver_opts:
      opt0: value0
      opt1: value1
  test1:
    driver: fake
    driver_opts:
      opt2: value2
      opt3: value3
networks:
  local:
    driver: bridge
  remote:
    driver: bridge
services:
  service-test-1:
    image: hello-world
    command: [echo, hi]
    container_name: this-is-a-test
    dns:
      - "8.8.8.8"
      - 8.8.4.4
    dns_search:
      - "notreal0.example.com"
      - notreal1.example.com
    tmpfs:
      - /tmp
      - /tmp1
    entrypoint: /bin/echo
    environment:
      testing: value1
      TESTING: value2
    expose:
      - "8080"
      - 8081
    labels:
      foo: bar
    logging:
      driver: syslog
      options:
        option1: value1
        option2: value2
    network_mode: bridge
    networks:
      local:
      remote:
        aliases:
          - a1
          - a2
    ports:
      - "8080:8081"
      - 9000
    volumes:
      - "~/test:/container/test"
      - test0:/test0
    working_dir: /working_dir
`

	jc := &JobCompose{}
	err := yaml.Unmarshal([]byte(expected), &jc)
	if err != nil {
		t.Error(err)
	}
	if jc.Version != "2" {
		t.Errorf("version was %s instead of '2'", jc.Version)
	}
	if len(jc.Networks) != 2 {
		t.Errorf("number of networks was %d instead of 1", len(jc.Networks))
	}
	if _, ok := jc.Networks["local"]; !ok {
		t.Errorf("could not find the 'local' network")
	}
	if jc.Networks["local"].Driver != "bridge" {
		t.Errorf("local network driver was %s instead of 'bridge'", jc.Networks["local"].Driver)
	}
	// if !jc.Networks["local"].EnableIPv6 {
	// 	t.Error("enable_ipv6 was false")
	// }
	if _, ok := jc.Networks["remote"]; !ok {
		t.Errorf("could not find the 'remote' network")
	}
	if jc.Networks["remote"].Driver != "bridge" {
		t.Errorf("local network driver was %s instead of 'bridge'", jc.Networks["local"].Driver)
	}
	// if jc.Networks["remote"].EnableIPv6 {
	// 	t.Error("enable_ipv6 was true")
	// }
	if len(jc.Volumes) != 2 {
		t.Errorf("number of volumes was %d instead of 2", len(jc.Volumes))
	}
	if jc.Volumes["test0"].Driver != "local" {
		t.Errorf("test0 volume driver was %s instead of 'local'", jc.Volumes["test0"].Driver)
	}
	if _, ok := jc.Volumes["test0"].Options["opt0"]; !ok {
		t.Error("opt0 volume driver option not found")
	}
	if _, ok := jc.Volumes["test0"].Options["opt1"]; !ok {
		t.Error("opt1 volume driver option not found")
	}
	if jc.Volumes["test1"].Driver != "fake" {
		t.Errorf("test1 volume driver was %s instead of 'fake'", jc.Volumes["test1"].Driver)
	}
	if _, ok := jc.Volumes["test1"].Options["opt2"]; !ok {
		t.Error("opt2 volume driver option not found")
	}
	if _, ok := jc.Volumes["test1"].Options["opt3"]; !ok {
		t.Error("opt3 volume driver option not found")
	}
	if len(jc.Services) != 1 {
		t.Errorf("number of services was %d instead of 1", len(jc.Services))
	}
	if _, ok := jc.Services["service-test-1"]; !ok {
		t.Errorf("service-test-1 was not found")
	}
	svc := jc.Services["service-test-1"]
	if svc.Image != "hello-world" {
		t.Errorf("image was %s", svc.Image)
	}
	if !reflect.DeepEqual(svc.Command, []string{"echo", "hi"}) {
		t.Errorf("command was '%s'", svc.Command)
	}
	if svc.ContainerName != "this-is-a-test" {
		t.Errorf("container name was %s", svc.ContainerName)
	}
	if len(svc.DNS) != 2 {
		t.Errorf("length of dns server list was %d instead of 2", len(svc.DNS))
	}
	if svc.DNS[0] != "8.8.8.8" {
		t.Errorf("first dns server was %s and not 8.8.8.8", svc.DNS[0])
	}
	if svc.DNS[1] != "8.8.4.4" {
		t.Errorf("second dns server was %s and not 8.8.4.4", svc.DNS[1])
	}
	if len(svc.DNSSearch) != 2 {
		t.Errorf("number of dns search domains was %d and not 2", len(svc.DNSSearch))
	}
	if svc.DNSSearch[0] != "notreal0.example.com" {
		t.Errorf("first dns search domain was %s", svc.DNSSearch[0])
	}
	if svc.DNSSearch[1] != "notreal1.example.com" {
		t.Errorf("second dns search domain was %s", svc.DNSSearch[1])
	}
	if len(svc.TMPFS) != 2 {
		t.Errorf("number if tmpfs'es was %d instead of 2", len(svc.TMPFS))
	}
	if svc.TMPFS[0] != "/tmp" {
		t.Errorf("first tmpfs was %s", svc.TMPFS[0])
	}
	if svc.TMPFS[1] != "/tmp1" {
		t.Errorf("second tmpfs was %s", svc.TMPFS[1])
	}
	if svc.EntryPoint != "/bin/echo" {
		t.Errorf("entrypoint was %s", svc.EntryPoint)
	}
	if len(svc.Environment) != 2 {
		t.Errorf("length of environment was %d instead of 2", len(svc.Environment))
	}
	if svc.Environment["testing"] != "value1" {
		t.Errorf("testing environment var was %s", svc.Environment["testing"])
	}
	if svc.Environment["TESTING"] != "value2" {
		t.Errorf("TESTING environment var was %s", svc.Environment["TESTING"])
	}
	if len(svc.Expose) != 2 {
		t.Errorf("number of exposed ports was %d instead of 2", len(svc.Expose))
	}
	if svc.Expose[0] != "8080" {
		t.Errorf("first exposed port was %s", svc.Expose[0])
	}
	if len(svc.Labels) != 1 {
		t.Errorf("number of labels was %d instead of 1", len(svc.Labels))
	}
	if _, ok := svc.Labels["foo"]; !ok {
		t.Error("label 'foo' not found")
	}
	if svc.Labels["foo"] != "bar" {
		t.Errorf("label 'foo' was %s instead of 'bar'", svc.Labels["foo"])
	}
	if svc.Logging.Driver != "syslog" {
		t.Errorf("logging driver was %s instead of 'syslog'", svc.Logging.Driver)
	}
	if len(svc.Logging.Options) != 2 {
		t.Errorf("number of logging driver options was %d instead of 2", len(svc.Logging.Options))
	}
	if _, ok := svc.Logging.Options["option1"]; !ok {
		t.Error("logging option option1 was not found")
	}
	if svc.Logging.Options["option1"] != "value1" {
		t.Errorf("logging option option1 was %s instead of 'value1'", svc.Logging.Options["option1"])
	}
	if svc.NetworkMode != "bridge" {
		t.Errorf("network mode is %s instead of 'bridge'", svc.NetworkMode)
	}
	if len(svc.Networks) != 2 {
		t.Errorf("number of service networks was %d instead of 2", len(svc.Networks))
	}
	if _, ok := svc.Networks["local"]; !ok {
		t.Error("local service network not found")
	}
	if _, ok := svc.Networks["remote"]; !ok {
		t.Errorf("remote service network not found")
	}
	if len(svc.Networks["remote"].Aliases) != 2 {
		t.Errorf("number of remote network aliases was %d instead of 2", len(svc.Networks["remote"].Aliases))
	}
	if svc.Networks["remote"].Aliases[0] != "a1" {
		t.Errorf("first remote network alias was %s instead of 'a1'", svc.Networks["remote"].Aliases[0])
	}
	if svc.Networks["remote"].Aliases[1] != "a2" {
		t.Errorf("second remote network alias was %s instead of 'a2'", svc.Networks["remote"].Aliases[1])
	}
	if len(svc.Ports) != 2 {
		t.Errorf("number of service ports was %d instead of 2", len(svc.Ports))
	}
	if svc.Ports[0] != "8080:8081" {
		t.Errorf("first service port was %s instead of '8080:8081'", svc.Ports[0])
	}
	if svc.Ports[1] != "9000" {
		t.Errorf("second service port was %s instead of '9000'", svc.Ports[1])
	}
	if len(svc.Volumes) != 2 {
		t.Errorf("number of service volumes as %d instead of 2", len(svc.Volumes))
	}
	if svc.Volumes[0] != "~/test:/container/test" {
		t.Errorf("first volume was %s instead of '~/test:/container/test'", svc.Volumes[0])
	}
	if svc.Volumes[1] != "test0:/test0" {
		t.Errorf("second volume was %s instead of 'test0:/test0'", svc.Volumes[1])
	}
	if svc.WorkingDir != "/working_dir" {
		t.Errorf("working directory was %s instead of /working_dir", svc.WorkingDir)
	}
}

func TestNew(t *testing.T) {
	jc, err := New("", "")
	if err != nil {
		t.Error(err)
	}
	if jc == nil {
		t.Error("New() returned nil")
	}
	if jc.Version != "2" {
		t.Errorf("version was %s", jc.Version)
	}
}

func TestConvertStep(t *testing.T) {
	jc, err := New("", "")
	if err != nil {
		t.Error(err)
	}
	jc.ConvertStep(&testJob.Steps[0], 0, testJob.Submitter, testJob.InvocationID)
	if len(jc.Services) != 1 {
		t.Errorf("number of services was %d and not 1", len(jc.Services))
	}
	if _, ok := jc.Services["step_0"]; !ok {
		t.Error("step_0 not found")
	}
	if _, ok := jc.Services["step_0"].Environment["FOO"]; !ok {
		t.Error("environment var FOO not found")
	}
	if jc.Services["step_0"].Environment["FOO"] != "BAR" {
		t.Errorf("FOO value was %s instead of 'BAR'", jc.Services["step_0"].Environment["FOO"])
	}
	if _, ok := jc.Services["step_0"].Environment["BAZ"]; !ok {
		t.Error("environment var BAZ not found")
	}
	if jc.Services["step_0"].Environment["BAZ"] != "1" {
		t.Errorf("BAZ value was %s instead of '1'", jc.Services["step_0"].Environment["BAZ"])
	}
	svc := jc.Services["step_0"]
	if svc.Image != "container-image-name-1:container-image-tag-1" {
		t.Errorf("image was %s", svc.Image)
	}
	if !reflect.DeepEqual(svc.Command, []string{"step-param-name-1", "step-param-value-1", "step-param-name-2", "step-param-value-2"}) {
		t.Errorf("command was %#v", svc.Command)
	}
}
