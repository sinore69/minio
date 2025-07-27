package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	minioClient *minio.Client
	bucketName  string
)

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found or failed to load it")
	}

	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	bucketName = os.Getenv("MINIO_BUCKET")

	if endpoint == "" || accessKey == "" || secretKey == "" || bucketName == "" {
		log.Fatal("One or more required environment variables are missing")
	}

	// Retry logic
	for {
		minioClient, err = minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: false,
		})
		if err == nil {
			break
		}
		log.Println("Waiting for MinIO to be ready:", err)
		time.Sleep(2 * time.Second)
	}
	log.Println("Connected to MinIO")

	// Ensure bucket exists
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		exists, err := minioClient.BucketExists(ctx, bucketName)
		if err == nil {
			if !exists {
				err := minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
				if err != nil {
					log.Fatalf("MakeBucket error: %v", err)
				}
				log.Printf("Created bucket: %s\n", bucketName)
			} else {
				log.Printf("Bucket already exists: %s\n", bucketName)
			}
			break
		}
		log.Printf("BucketExists error: %v (attempt %d/10). Retrying in 2s...", err, i+1)
		time.Sleep(2 * time.Second)

		if i == 9 {
			log.Fatalf("Failed to connect to MinIO after 10 attempts: %v", err)
		}
	}

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/download", downloadHandler)
	http.HandleFunc("/list", listHandler)

	log.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	objectName := r.URL.Query().Get("key")
	if objectName == "" {
		http.Error(w, "Missing 'key' query parameter", http.StatusBadRequest)
		return
	}

	_, err := minioClient.PutObject(ctx, bucketName, objectName, r.Body, -1, minio.PutObjectOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Upload failed: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Uploaded %s successfully\n", objectName)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	objectName := r.URL.Query().Get("key")
	if objectName == "" {
		http.Error(w, "Missing 'key' query parameter", http.StatusBadRequest)
		return
	}

	object, err := minioClient.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Download failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer object.Close()

	w.Header().Set("Content-Disposition", "inline")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, object)
}

func listHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	for obj := range minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			http.Error(w, obj.Err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, obj.Key)
	}
}

