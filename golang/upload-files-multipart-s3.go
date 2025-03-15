package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
	"golang.org/x/sync/semaphore"
)

// 🔥 Reducir la carga del Garbage Collector para mejorar rendimiento
func init() {
	debug.SetGCPercent(10) // 🔥 Menos interrupciones del GC
}

// 🔹 Configuración de concurrencia (máximo 100 subidas simultáneas)
var sem = semaphore.NewWeighted(100)

// 🔹 Cliente HTTP optimizado para AWS SDK
var customHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        500, // 🔥 Aumentamos conexiones HTTP
		MaxIdleConnsPerHost: 500,
		MaxConnsPerHost:     500,
	},
	Timeout: 60 * time.Second, // 🔥 Mayor tiempo de espera para evitar fallos en conexiones lentas
}

// 🔹 Cargar variables de entorno desde `.env`
func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("❌ ERROR: No se pudo cargar el archivo .env")
	}
}

// 🔹 Crear cliente S3 con credenciales
func createS3Client() (*s3.Client, error) {
	accessKey := os.Getenv("ACCESS_KEY_ID")
	secretKey := os.Getenv("SECRET_ACCESS_KEY")

	if accessKey == "" || secretKey == "" {
		log.Fatal("❌ ERROR: Las credenciales de AWS no están configuradas en las variables de entorno.")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithHTTPClient(customHTTPClient),
		config.WithRetryer(func() aws.Retryer { return retry.NewStandard() }),
	)
	if err != nil {
		return nil, fmt.Errorf("error cargando configuración de AWS: %v", err)
	}

	return s3.NewFromConfig(cfg), nil
}

// 🔹 Subida Multipart con concurrencia para partes grandes
func uploadFileMultipart(client *s3.Client, bucket, key, filePath string, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("❌ Error abriendo %s: %v", filePath, err)
		return
	}
	defer file.Close()

	partSize := int64(5 * 1024 * 1024) // 🔥 5MB por parte
	upload, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("❌ Error iniciando subida multipart para %s: %v", filePath, err)
		return
	}

	var parts []types.CompletedPart
	var uploadWg sync.WaitGroup // 🔥 Subir partes en paralelo
	partNumber := int32(1)

	for {
		buffer := make([]byte, partSize)
		n, err := io.ReadFull(file, buffer)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			log.Printf("❌ Error leyendo %s: %v", filePath, err)
			return
		}

		uploadWg.Add(1)
		go func(partNumber int32, buffer []byte, size int) { // 🔥 Subir en paralelo
			defer uploadWg.Done()

			resp, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   upload.UploadId,
				PartNumber: aws.Int32(partNumber),
				Body:       bytes.NewReader(buffer[:size]),
			})

			if err != nil {
				log.Printf("❌ Error subiendo parte %d de %s: %v", partNumber, filePath, err)
				return
			}

			parts = append(parts, types.CompletedPart{
				ETag:       resp.ETag,
				PartNumber: aws.Int32(partNumber),
			})
		}(partNumber, buffer, n)

		partNumber++
	}

	uploadWg.Wait() // 🔥 Esperar a que todas las partes se suban

	// 🔹 Ordenar las partes antes de completar la subida
	sort.Slice(parts, func(i, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})

	_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: upload.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})

	if err != nil {
		log.Printf("❌ Error finalizando subida multipart para %s: %v", filePath, err)
	} else {
		//log.Printf("✅ Subida multipart completada: %s → s3://%s/%s", filePath, bucket, key)
	}
}

// 🔹 Subida normal para archivos pequeños (<5MB)
func uploadFile(client *s3.Client, bucket, key, filePath string, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("❌ Error abriendo %s: %v", filePath, err)
		return
	}
	defer file.Close()

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})

	if err != nil {
		log.Printf("❌ Error subiendo %s: %v", filePath, err)
	} else {
		//log.Printf("✅ Subido correctamente: %s → s3://%s/%s", filePath, bucket, key)
	}
}

// 🔹 Subir archivos en paralelo
func uploadFilesParallel(directory string) {
	startTime := time.Now()

	files, _ := listFiles(directory)
	bucketName := os.Getenv("BUCKET_NAME")
	bucketKey := os.Getenv("BUCKET_KEY")

	client, _ := createS3Client()

	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		s3Key := filepath.ToSlash(filepath.Join(bucketKey, filepath.Base(file)))

		if stat, _ := os.Stat(file); stat.Size() > 5*1024*1024 {
			go uploadFileMultipart(client, bucketName, s3Key, file, &wg)
		} else {
			go uploadFile(client, bucketName, s3Key, file, &wg)
		}
	}

	wg.Wait()
	fmt.Printf("🚀 Tiempo total: %s\n", time.Since(startTime))
}

func main() {
	loadEnv()
	uploadFilesParallel(os.Getenv("directory_path"))
}

// Listar archivos en un directorio
func listFiles(directory string) ([]string, error) {
	var files []string
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, filepath.Join(directory, entry.Name()))
		}
	}
	return files, nil
}
