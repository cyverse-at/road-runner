package main

import "github.com/cyverse-de/model"

var testJob = &model.Job{
	ID:           "test-job-id",
	InvocationID: "test-invocation-id",
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
					ID:   "container-id-1",
					Name: "container-name-1",
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

// func TestDownloadInputs(t *testing.T) {
// 	u := NewTestJobUpdatePublisher(false)
// 	sc, err := downloadInputs(u, testJob)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if sc != messaging.Success {
// 		t.Errorf("status code was %d instead of %d", sc, messaging.Success)
// 	}
// }
//
// func TestRunAllSteps(t *testing.T) {
// 	u := NewTestJobUpdatePublisher(false)
// 	e := make(chan messaging.StatusCode, 0)
// 	sc, err := runAllSteps(u, testJob, e)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if sc != messaging.Success {
// 		t.Errorf("status code was %d instead of %d", sc, messaging.Success)
// 	}
// }
//
// func TestUploadOutputs(t *testing.T) {
// 	u := NewTestJobUpdatePublisher(false)
// 	sc, err := uploadOutputs(u, testJob)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	if sc != messaging.Success {
// 		t.Errorf("status code was %d instead of %d", sc, messaging.Success)
// 	}
// }
