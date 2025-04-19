package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"log"
)

var newImageUploadedTopicArn = "arn:aws:sns:us-east-1:116981782358:new-image-uploaded"

func handler(ctx context.Context, s3Event events.S3Event) error {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("failed to load default config: %s\n", err)
		return err
	}
	snsClient := sns.NewFromConfig(sdkConfig)

	for _, record := range s3Event.Records {
		bucket := record.S3.Bucket.Name
		key := record.S3.Object.URLDecodedKey
		log.Printf("success receive s3 event trigger for bucket: %s and key: %s\n", bucket, key)

		payload := struct {
			Bucket string `json:"bucket"`
			Key    string `json:"key"`
		}{
			Bucket: bucket,
			Key:    key,
		}

		m, err := json.Marshal(payload)
		if err != nil {
			log.Printf("failed to marshal payload: %s\n", err)
			return err
		}
		message := string(m)

		resp, err := snsClient.Publish(ctx, &sns.PublishInput{
			Message:  &message,
			TopicArn: &newImageUploadedTopicArn,
		})
		if err != nil {
			log.Printf("failed to publish message to SNS: %s\n", err)
			return err
		}

		log.Printf("message sent to SNS with message ID: %s\n", *resp.MessageId)
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
