package ecsutils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"
)

type ECSTask struct {
	ServiceKey      string `json:"-"` // Should be private for output
	PlatformVersion string `json:"platformVersion"`
	TaskDefinition  string `json:"taskDefinition"`
	Cluster         string `json:"cluster"`
}

func NewECSTask(serviceKey string) ECSTask {
	return ECSTask{
		ServiceKey:      serviceKey,
		PlatformVersion: "1.4.0", // User can override this if needed
	}
}

func (e *ECSTask) RunFargateTask(serviceKey string) error {
	ecsService, err := NewECSSession(e.ServiceKey)
	if err != nil {
		return err
	}

	// TODO Find a way to look up the security group and subnets assigned for the taskdefinition/cluster and use
	var securityGroup, subnet1, subnet2 string

	_, err = ecsService.RunTask(&ecs.RunTaskInput{
		// CapacityProviderStrategy: nil,
		Cluster: aws.String(e.Cluster),
		// Count:                    nil,
		// EnableECSManagedTags:     nil,
		Group:      aws.String("family:" + e.TaskDefinition),
		LaunchType: aws.String("FARGATE"),
		NetworkConfiguration: &ecs.NetworkConfiguration{
			AwsvpcConfiguration: &ecs.AwsVpcConfiguration{
				AssignPublicIp: aws.String("ENABLED"),
				SecurityGroups: []*string{
					aws.String(securityGroup),
				},
				Subnets: []*string{
					aws.String(subnet1),
					aws.String(subnet2),
				},
			},
		},
		// Overrides:            nil,
		// PlacementConstraints: nil,
		// PlacementStrategy:    nil,
		PlatformVersion: aws.String(e.PlatformVersion),
		// PropagateTags:        nil,
		// ReferenceId:          nil,
		// StartedBy:            nil,
		// Tags:                 nil,
		TaskDefinition: aws.String(e.TaskDefinition),
	})

	// log.Println(taskOutput) // What do we want to return from the function?
	if err != nil {
		return err
	}

	return nil
}
