# grpc-folder-transfer

Go + gRPC로 구현한 폴더 업로드/다운로드 전송 도구.
폴더 업로드 시 서버의 /tmp/grpc-upload에 저장됨.
폴더 다운로드 시 서버의 /tmp/grpc-download로부터 다운로드됨.

---

## 실행파일 위치

```
bin/grpc-server
bin/grpc-client
bin/grpc-win-client.exe
```

---

## 빌드 방법

**서버**
```bash
go build -o bin/grpc-server ./server/
```

**클라이언트(Linux)**
```bash
go build -o bin/grpc-client ./client/
```

**클라이언트(Windows)**
```bash
GOOS=windows GOARCH=amd64 go build -o bin/grpc-win-client.exe ./client/
```

---

## 실행 방법

**서버**
```bash
./bin/grpc-server
```

**클라이언트(Linux)**
```bash
# 업로드
./bin/grpc-client -mode=upload -src=path/to/folder

# 다운로드
./bin/grpc-client -mode=download -src=remote-folder -dest=path/to/save
```

**클라이언트(Windows)**
```bash
# 업로드
.\bin\grpc-win-client.exe -mode=upload -src=path\to\folder

# 다운로드
.\bin\grpc-win-client.exe -mode=download -src=remote-folder -dest=path\to\save
```
