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
)
 
const (
  serverAddr = "192.168.10.10:50051"
  chunkSize  = 1024 * 1024 // 1MB
)
 
// ── UploadFolder ────────────────────────────────────────
func uploadFolder(client pb.FolderTransferClient, localFolder string) {
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
  defer cancel()
 
  stream, err := client.UploadFolder(ctx)
  if err != nil { log.Fatalf("UploadFolder 스트림 생성 실패: %v", err) }
 
  totalFiles := 0
  err = filepath.Walk(localFolder, func(path string, info os.FileInfo, err error) error {
    if err != nil || info.IsDir() { return err }
 
    relPath, _ := filepath.Rel(filepath.Dir(localFolder), path)
    f, err := os.Open(path)
    if err != nil { return fmt.Errorf("파일 열기 실패 %s: %w", path, err) }
    defer f.Close()
 
    log.Printf("[Upload] 전송 시작: %s", relPath)
    buf := make([]byte, chunkSize)
    chunkIdx := int32(0)
 
    for {
      n, readErr := f.Read(buf)
      if n > 0 {
        isLast := readErr == io.EOF
        if err := stream.Send(&pb.FileChunk{
          FilePath:   relPath,
          Content:    buf[:n],
          IsLast:     isLast,
          FileSize:   info.Size(),
          ChunkIndex: chunkIdx,
        }); err != nil {
          return fmt.Errorf("청크 전송 실패: %w", err)
        }
        chunkIdx++
      }
      if readErr == io.EOF { break }
      if readErr != nil { return readErr }
    }
    totalFiles++
    log.Printf("[Upload] 완료: %s (%d 청크)", relPath, chunkIdx)
    return nil
  })
 
  if err != nil { log.Fatalf("폴더 순회 오류: %v", err) }
 
  resp, err := stream.CloseAndRecv()
  if err != nil { log.Fatalf("응답 수신 실패: %v", err) }
  log.Printf("[Upload] 결과: %s", resp.Message)
  log.Printf("[Upload] 파일 수: %d, 총 크기: %.2f MB", resp.FilesCount, float64(resp.TotalBytes)/1024/1024)
}
 
// ── DownloadFolder ──────────────────────────────────────
func downloadFolder(client pb.FolderTransferClient, remotePath, localDest string) {
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
  defer cancel()
 
  stream, err := client.DownloadFolder(ctx, &pb.DownloadRequest{FolderPath: remotePath})
  if err != nil { log.Fatalf("DownloadFolder 스트림 생성 실패: %v", err) }
 
  fileHandles := make(map[string]*os.File)
  defer func() { for _, f := range fileHandles { f.Close() } }()
 
  for {
    chunk, err := stream.Recv()
    if err == io.EOF { break }
    if err != nil { log.Fatalf("청크 수신 오류: %v", err) }
 
    destPath := filepath.Join(localDest, chunk.FilePath)
    f, exists := fileHandles[destPath]
    if !exists {
      os.MkdirAll(filepath.Dir(destPath), 0755)
      f, err = os.Create(destPath)
      if err != nil { log.Fatalf("파일 생성 실패: %v", err) }
      fileHandles[destPath] = f
      log.Printf("[Download] 수신 시작: %s (%.2f MB)", chunk.FilePath, float64(chunk.FileSize)/1024/1024)
    }
    f.Write(chunk.Content)
    if chunk.IsLast {
      f.Close()
      delete(fileHandles, destPath)
      log.Printf("[Download] 완료: %s", chunk.FilePath)
    }
  }
  log.Println("[Download] 전체 다운로드 완료!")
}
 
func main() {
  mode := flag.String("mode", "upload", "실행 모드: upload | download")
  src  := flag.String("src",  "",       "업로드: 로컬 폴더 | 다운로드: 서버 폴더 경로")
  dest := flag.String("dest", "/tmp/grpc-downloaded", "다운로드 저장 경로")
  flag.Parse()
 
  if *src == "" { log.Fatal("-src 인자가 필요합니다") }
 
  // gRPC 연결
  conn, err := grpc.NewClient(serverAddr,
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithDefaultCallOptions(
      grpc.MaxCallRecvMsgSize(64*1024*1024),
      grpc.MaxCallSendMsgSize(64*1024*1024),
    ),
  )
  if err != nil { log.Fatalf("서버 연결 실패: %v", err) }
  defer conn.Close()
 
  client := pb.NewFolderTransferClient(conn)
  log.Printf("서버 연결 성공: %s", serverAddr)
 
  switch *mode {
  case "upload":
    uploadFolder(client, *src)
  case "download":
    downloadFolder(client, *src, *dest)
  default:
    log.Fatalf("알 수 없는 모드: %s", *mode)
  }
}
