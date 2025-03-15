package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

//var sem = semaphore.NewWeighted(50)

// Cargar variables de entorno desde .env
func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error cargando el archivo .env")
	}
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

// Subir un archivo a S3
func uploadFile(client *s3.Client, bucket, key, filePath string, wg *sync.WaitGroup) {
	defer wg.Done()

	// if err := sem.Acquire(context.TODO(), 1); err != nil {
	// 	log.Printf("❌ Error adquiriendo el semáforo para %s: %v", filePath, err)
	// 	return
	// }
	// defer sem.Release(1)

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("❌ Error abriendo el archivo %s: %v", filePath, err)
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
		//log.Printf("✅ Subido: %s → s3://%s/%s", filePath, bucket, key)
	}
}

// Subir archivos en paralelo usando goroutines
func uploadFilesParallel(directory string) {
	files, err := listFiles(directory)
	if err != nil {
		log.Fatalf("Error listando archivos: %v", err)
	}

	loadEnv() // Cargar variables desde .env

	bucketName := os.Getenv("BUCKET_NAME")
	bucketKey := os.Getenv("BUCKET_KEY")
	accessKey := os.Getenv("ACCESS_KEY_ID")
	secretKey := os.Getenv("SECRET_ACCESS_KEY")

	var customHTTPClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,  // 🔥 Aumenta el número máximo de conexiones inactivas
			MaxIdleConnsPerHost: 100,  // 🔥 Aumenta el número máximo de conexiones por host
			MaxConnsPerHost:     100,  // 🔥 Aumenta el número máximo de conexiones por host
		},
		Timeout: 30 * time.Second, // 🔥 Evita bloqueos de conexión
	}

	// Configurar credenciales manualmente
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithHTTPClient(customHTTPClient),
	)
	if err != nil {
		log.Fatalf("Error cargando la configuración de AWS: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		///s3Key := filepath.Join(bucketKey, filepath.Base(file)) // Mantener la estructura en S3

		s3Key := bucketKey + "/" + filepath.Base(file)

		go uploadFile(client, bucketName, s3Key, file, &wg)
	}

	wg.Wait() // Esperar que todas las subidas terminen
}

func main() {
	fmt.Println("🚀 Subiendo archivos a S3...")
	startTime := time.Now() // 🔹 Iniciar conteo de tiempo global

	uploadFilesParallel("C:\\Users\\nitro\\Documents\\programming\\pythonProjects\\upload-files-to-s3\\directory")

	elapsedTime := time.Since(startTime) // 🔹 Calcular tiempo total
	fmt.Printf("🚀 Tiempo total de ejecución: %s\n", elapsedTime)
	fmt.Println("✅ Proceso completado.")
}