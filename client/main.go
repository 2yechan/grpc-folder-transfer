package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	pb "folder-transfer/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

const chunkSize = 1024 * 1024 // 1MB

// ── UploadFolder ────────────────────────────────────────
func uploadFolder(client pb.FolderTransferClient, localFolder string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.UploadFolder(ctx)
	if err != nil {
		log.Fatalf("UploadFolder 스트림 생성 실패: %v", err)
	}

	totalFiles := 0
	err = filepath.Walk(localFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		relPath, _ := filepath.Rel(filepath.Dir(localFolder), path)
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("파일 열기 실패 %s: %w", path, err)
		}
		defer f.Close()

		log.Printf("[Upload] 전송 시작: %s", relPath)
		buf := make([]byte, chunkSize)
		sent := false

		for {
			n, readErr := f.Read(buf)
			if n > 0 {
				if err := stream.Send(&pb.FileChunk{
					FilePath: relPath,
					Content:  buf[:n],
					FileSize: info.Size(),
				}); err != nil {
					return fmt.Errorf("청크 전송 실패: %w", err)
				}
				sent = true
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return readErr
			}
		}

		// 빈 파일도 청크 한 개를 보내 수신측에서 생성되도록 함
		if !sent {
			if err := stream.Send(&pb.FileChunk{FilePath: relPath, FileSize: 0}); err != nil {
				return fmt.Errorf("빈 파일 전송 실패: %w", err)
			}
		}

		totalFiles++
		log.Printf("[Upload] 완료: %s", relPath)
		return nil
	})

	if err != nil {
		log.Fatalf("폴더 순회 오류: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("응답 수신 실패: %v", err)
	}
	log.Printf("[Upload] 결과: %s", resp.Message)
	log.Printf("[Upload] 파일 수: %d, 총 크기: %.2f MB", resp.FilesCount, float64(resp.TotalBytes)/1024/1024)
}

// ── DownloadFolder ──────────────────────────────────────
func downloadFolder(client pb.FolderTransferClient, remotePath, localDest string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.DownloadFolder(ctx, &pb.DownloadRequest{FolderPath: remotePath})
	if err != nil {
		log.Fatalf("DownloadFolder 스트림 생성 실패: %v", err)
	}

	var cur *os.File
	var curPath string
	defer func() {
		if cur != nil {
			cur.Close()
		}
	}()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("청크 수신 오류: %v", err)
		}

		// 파일 경로가 바뀌면 이전 파일을 닫고 새로 연다 (핸들을 항상 1개만 유지)
		if cur == nil || chunk.FilePath != curPath {
			if cur != nil {
				cur.Close()
			}
			destPath := filepath.Join(localDest, chunk.FilePath)
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				log.Fatalf("디렉토리 생성 실패: %v", err)
			}
			cur, err = os.Create(destPath)
			if err != nil {
				log.Fatalf("파일 생성 실패: %v", err)
			}
			curPath = chunk.FilePath
			log.Printf("[Download] 수신: %s (%.2f MB)", chunk.FilePath, float64(chunk.FileSize)/1024/1024)
		}

		if _, err := cur.Write(chunk.Content); err != nil {
			log.Fatalf("파일 쓰기 실패: %v", err)
		}
	}
	log.Println("[Download] 전체 다운로드 완료!")
}

func main() {
	addr := flag.String("addr", "192.168.10.10:50051", "gRPC 서버 주소 (host:port)")
	mode := flag.String("mode", "upload", "실행 모드: upload | download")
	src := flag.String("src", "", "업로드: 로컬 폴더 | 다운로드: 서버 폴더 경로")
	dest := flag.String("dest", "/tmp/grpc-downloaded", "다운로드 저장 경로")
	flag.Parse()

	if *src == "" {
		log.Fatal("-src 인자가 필요합니다")
	}

	// gRPC 연결
	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
			grpc.UseCompressor(gzip.Name),
		),
	)
	if err != nil {
		log.Fatalf("서버 연결 실패: %v", err)
	}
	defer conn.Close()

	client := pb.NewFolderTransferClient(conn)
	log.Printf("서버 연결 성공: %s", *addr)

	switch *mode {
	case "upload":
		uploadFolder(client, *src)
	case "download":
		downloadFolder(client, *src, *dest)
	default:
		log.Fatalf("알 수 없는 모드: %s", *mode)
	}
}
