package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	pb "folder-transfer/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/reflection"
)

const (
	port        = ":50051"
	uploadDir   = "/tmp/grpc-upload"   // 업로드 저장 디렉토리
	downloadDir = "/tmp/grpc-download" // 다운로드 제공 디렉토리
	chunkSize   = 1024 * 1024          // 1MB 청크
)

// FolderServer: gRPC 서비스 구현체
type FolderServer struct {
	pb.UnimplementedFolderTransferServer
}

// ── UploadFolder: Client Streaming ──────────────────────
func (s *FolderServer) UploadFolder(stream pb.FolderTransfer_UploadFolderServer) error {
	var totalBytes int64
	filesCount := 0

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
			return fmt.Errorf("수신 오류: %w", err)
		}

		// 안전한 경로 처리 (Path Traversal 방지)
		safePath := filepath.Join(uploadDir, filepath.Clean("/"+chunk.FilePath))
		if !strings.HasPrefix(safePath, uploadDir) {
			return fmt.Errorf("허용되지 않는 경로: %s", chunk.FilePath)
		}

		// 파일 경로가 바뀌면 이전 파일을 닫고 새로 연다 (핸들을 항상 1개만 유지)
		if cur == nil || safePath != curPath {
			if cur != nil {
				cur.Close()
			}
			if err := os.MkdirAll(filepath.Dir(safePath), 0755); err != nil {
				return fmt.Errorf("디렉토리 생성 실패: %w", err)
			}
			cur, err = os.Create(safePath)
			if err != nil {
				return fmt.Errorf("파일 생성 실패: %w", err)
			}
			curPath = safePath
			filesCount++
			log.Printf("[Upload] 파일 수신 시작: %s (크기: %d bytes)", chunk.FilePath, chunk.FileSize)
		}

		n, err := cur.Write(chunk.Content)
		if err != nil {
			return fmt.Errorf("파일 쓰기 실패: %w", err)
		}
		totalBytes += int64(n)
	}

	msg := fmt.Sprintf("업로드 완료: %d개 파일, %.2f MB", filesCount, float64(totalBytes)/1024/1024)
	log.Println(msg)
	return stream.SendAndClose(&pb.UploadResponse{
		Success:    true,
		Message:    msg,
		FilesCount: int32(filesCount),
		TotalBytes: totalBytes,
	})
}

// ── DownloadFolder: Server Streaming ────────────────────
func (s *FolderServer) DownloadFolder(req *pb.DownloadRequest, stream pb.FolderTransfer_DownloadFolderServer) error {
	// 다운로드 응답 스트림도 gzip으로 압축
	if err := grpc.SetSendCompressor(stream.Context(), gzip.Name); err != nil {
		log.Printf("압축기 설정 실패(무시): %v", err)
	}

	baseDir := filepath.Join(downloadDir, filepath.Clean("/"+req.FolderPath))
	log.Printf("[Download] 요청 폴더: %s", baseDir)

	return filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		relPath, _ := filepath.Rel(downloadDir, path)
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		log.Printf("[Download] 전송 시작: %s", relPath)
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
					return fmt.Errorf("전송 오류: %w", err)
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
				return fmt.Errorf("빈 파일 전송 오류: %w", err)
			}
		}

		log.Printf("[Download] 전송 완료: %s", relPath)
		return nil
	})
}

func main() {
	// 디렉토리 생성
	for _, dir := range []string{uploadDir, downloadDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("디렉토리 생성 실패: %v", err)
		}
	}

	// gRPC 서버 시작
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Listen 실패: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024), // 최대 수신 64MB
		grpc.MaxSendMsgSize(64*1024*1024), // 최대 전송 64MB
	)

	pb.RegisterFolderTransferServer(grpcServer, &FolderServer{})
	reflection.Register(grpcServer) // grpcurl 등 디버깅 도구 지원

	log.Printf("gRPC 서버 시작: %s (업로드→%s, 다운로드←%s)", port, uploadDir, downloadDir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("서버 오류: %v", err)
	}
}
