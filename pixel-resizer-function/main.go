package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nfnt/resize"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"log"
	"path/filepath"
	"strings"
	"sync"
)

var s3Client *s3.Client

const (
	ResizedFolder = "resized/"
)

type messageBody struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type resizeTask struct {
	SizeName string
	Width    uint
}

func handler(ctx context.Context, event events.SQSEvent) error {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("failed to load default config: %s\n", err)
		return err
	}
	s3Client = s3.NewFromConfig(sdkConfig)

	for _, record := range event.Records {
		log.Printf("received message from SQS with messageID: %s, body: %s\n", record.MessageId, record.Body)

		var body messageBody
		err := json.Unmarshal([]byte(record.Body), &body)
		if err != nil {
			log.Printf("failed to unmarshal this messageID %s: %s\n", record.MessageId, err)
			return err
		}

		objOutput, err := getObject(ctx, body.Bucket, body.Key)
		if err != nil {
			log.Printf("failed to get object from S3: %s\n", err)
			return err
		}
		defer objOutput.Body.Close()
		log.Printf("content type: %s\n", *objOutput.ContentType)
		log.Printf("content length: %d\n", *objOutput.ContentLength)

		// Decode the image
		img, format, err := image.Decode(objOutput.Body)
		if err != nil {
			log.Printf("failed to decode image: %s\n", err)
			return err
		}
		log.Printf("decoded image format: %s\n", format)

		// Resize the image
		resizeTasks := []resizeTask{
			{"thumbnail", 100},
			{"medium", 500},
			{"large", 1000},
		}

		var wg sync.WaitGroup

		for _, task := range resizeTasks {
			wg.Add(1)
			go func(task resizeTask) {
				defer wg.Done()

				resizedImg := resize.Resize(task.Width, 0, img, resize.Lanczos3)
				if err != nil {
					log.Printf("failed to resize image: %s\n", err)
					return
				}

				jpegImg, err := convertToJPEG(resizedImg)
				if err != nil {
					log.Printf("failed to convert image: %s\n", err)
					return
				}

				err = putObject(ctx, body.Bucket, ResizedFolder+extractFilename(body.Key)+"/"+task.SizeName+".jpg", jpegImg)
				if err != nil {
					log.Printf("failed to put object to S3: %s\n", err)
					return
				}
			}(task)
		}

		wg.Wait()

		log.Printf("all resize tasks completed for image: %s\n", body.Key)
	}

	return nil
}

// extractFilename extracts the filename from the input string
func extractFilename(input string) string {
	// Extract the base name (e.g., aaa.jpg)
	base := filepath.Base(input)
	// Remove the file extension (e.g., aaa)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	return name
}

// getObject retrieves an object from S3
func getObject(ctx context.Context, bucket string, key string) (*s3.GetObjectOutput, error) {
	output, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Printf("failed to get object from S3: %s\n", err)
		return nil, err
	}

	log.Printf("successfully retrieved object from S3 bucket: %s and key: %s\n", bucket, key)

	return output, nil
}

// putObject uploads an object to S3
func putObject(ctx context.Context, bucket string, key string, body []byte) error {
	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		log.Printf("failed to put object to S3: %s\n", err)
		return err
	}

	log.Printf("successfully put object to S3 bucket: %s and key: %s\n", bucket, key)

	return nil
}

// convertToJPEG converts an image to JPEG format
func convertToJPEG(img image.Image) ([]byte, error) {
	var buf bytes.Buffer

	// Set JPEG options (quality 100)
	options := &jpeg.Options{Quality: 100}

	err := jpeg.Encode(&buf, img, options)
	if err != nil {
		log.Printf("failed to convert image to JPEG: %s\n", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

func main() {
	lambda.Start(handler)
}
