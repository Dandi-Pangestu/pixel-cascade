package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var s3Client *s3.Client
var dynamoClient *dynamodb.Client
var tableName = "pixel-metadata"

type messageBody struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func handler(ctx context.Context, event events.SQSEvent) error {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("failed to load default config: %s\n", err)
		return err
	}
	s3Client = s3.NewFromConfig(sdkConfig)
	dynamoClient = dynamodb.NewFromConfig(sdkConfig)

	for _, record := range event.Records {
		log.Printf("received message from SQS with messageID: %s, body: %s\n", record.MessageId, record.Body)

		var body messageBody
		err := json.Unmarshal([]byte(record.Body), &body)
		if err != nil {
			log.Printf("failed to unmarshal this messageID %s: %s\n", record.MessageId, err)
			return err
		}

		metadata, err := generateMetadata(ctx, body.Bucket, body.Key)
		if err != nil {
			log.Printf("failed to extract metadata: %s\n", err)
			return err
		}

		// Creating DynamoDB item
		err = createDynamoDBItem(ctx, map[string]types.AttributeValue{
			"name":     &types.AttributeValueMemberS{Value: extractFilename(body.Key)},
			"bucket":   &types.AttributeValueMemberS{Value: body.Bucket},
			"metadata": &types.AttributeValueMemberM{Value: convertToAttributeValue(metadata)},
		})
		if err != nil {
			log.Printf("failed to create DynamoDB item: %s\n", err)
			return err
		}
	}

	return nil
}

// generateMetadata generates metadata for the image file
func generateMetadata(ctx context.Context, bucket, key string) (map[string]string, error) {
	objOutput, err := getObject(ctx, bucket, key)
	if err != nil {
		log.Printf("failed to get object from S3: %s\n", err)
		return nil, err
	}
	defer objOutput.Body.Close()
	log.Printf("content type: %s\n", *objOutput.ContentType)
	log.Printf("content length: %d\n", *objOutput.ContentLength)

	// Read all bytes from S3 object
	bodyBytes, err := io.ReadAll(objOutput.Body)
	if err != nil {
		log.Printf("failed to read object body: %s\n", err)
		return nil, err
	}

	// Write bytes to temp file
	localFilePath := filepath.Join("/tmp", filepath.Base(key))
	err = os.WriteFile(localFilePath, bodyBytes, 0644)
	if err != nil {
		log.Printf("failed to write to local file: %s\n", err)
		return nil, err
	}
	defer os.Remove(localFilePath)

	// Open file for reading
	file, err := os.Open(localFilePath)
	if err != nil {
		log.Printf("failed to open local file: %s\n", err)
		return nil, err
	}
	defer file.Close()

	// Decode image to get dimensions
	cfg, format, err := image.DecodeConfig(file)
	if err != nil {
		log.Printf("failed to decode image: %s\n", err)
		return nil, err
	}

	// Open the file again to extract EXIF data
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		log.Printf("failed to get file info: %s\n", err)
		return nil, err
	}

	// Construct metadata map
	metadata := map[string]string{
		"width":         strconv.Itoa(cfg.Width),
		"height":        strconv.Itoa(cfg.Height),
		"format":        format,
		"file_size":     strconv.FormatInt(fileInfo.Size(), 10), // File size in bytes, // File size in bytes
		"file_name":     fileInfo.Name(),
		"last_modified": fileInfo.ModTime().Format(time.RFC3339),
	}

	return metadata, nil
}

// convertToAttributeValue converts a map of string to AttributeValue
func convertToAttributeValue(data map[string]string) map[string]types.AttributeValue {
	attributeValue := make(map[string]types.AttributeValue)
	for key, value := range data {
		attributeValue[key] = &types.AttributeValueMemberS{Value: value}
	}
	return attributeValue
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

// createDynamoDBItem creates a new item in DynamoDB
func createDynamoDBItem(ctx context.Context, item map[string]types.AttributeValue) error {
	_, err := dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item:      item,
	})
	if err != nil {
		log.Printf("failed to put item in DynamoDB: %s\n", err)
		return err
	}

	log.Printf("successfully put item in DynamoDB table: %s\n", tableName)

	return nil
}

func main() {
	lambda.Start(handler)
}
