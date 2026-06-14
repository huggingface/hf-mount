# Windows CSI Driver Build Instructions

This document describes how to manually build Windows CSI driver images for hf-mount. Currently, Windows images are not built automatically in CI due to infrastructure limitations.

## Prerequisites

- Windows 10/11 or Windows Server 2019+
- Docker Desktop for Windows with Windows container support enabled
- Git
- Rust toolchain (1.89+)

## Build Steps

### 1. Cross-compile from Linux/macOS

If you're on Linux or macOS, you can cross-compile Windows binaries:

```bash
# Add Windows target
rustup target add x86_64-pc-windows-gnu

# Build Windows NFS binary (FUSE not supported on Windows)
cargo build --release --target x86_64-pc-windows-gnu --features nfs,vendored-openssl

# Build CSI driver
cargo build --release --target x86_64-pc-windows-gnu --features nfs,vendored-openssl --bin hf-mount-csi
```

The compiled binaries will be in:
- `target/x86_64-pc-windows-gnu/release/hf-mount-nfs.exe`
- `target/x86_64-pc-windows-gnu/release/hf-mount-csi.exe`

### 2. Build on Windows

For native Windows builds:

```powershell
# Install Rust (if not already installed)
# Download from https://rustup.rs/

# Clone the repository
git clone https://github.com/huggingface/hf-mount.git
cd hf-mount

# Build Windows binaries (NFS only, no FUSE on Windows)
cargo build --release --features nfs,vendored-openssl

# Build CSI driver specifically
cargo build --release --features nfs,vendored-openssl --bin hf-mount-csi
```

### 3. Build Windows Docker Image

Create a `Dockerfile.windows` in the repository root:

```dockerfile
# Use Windows Server Core base image
FROM mcr.microsoft.com/windows/servercore:ltsc2022

# Install Visual C++ Redistributable
RUN powershell -Command \
    $ProgressPreference = 'SilentlyContinue'; \
    Invoke-WebRequest -Uri https://aka.ms/vs/17/release/vc_redist.x64.exe -OutFile vc_redist.x64.exe; \
    Start-Process vc_redist.x64.exe -ArgumentList '/install /quiet /norestart' -Wait; \
    Remove-Item vc_redist.x64.exe

# Copy compiled binaries
COPY target/x86_64-pc-windows-gnu/release/hf-mount-csi.exe /hf-mount-csi.exe

# Set entrypoint
ENTRYPOINT ["hf-mount-csi.exe"]
CMD ["--endpoint=0.0.0.0:50051"]
```

Build the Windows container image:

```powershell
# Switch to Windows containers in Docker Desktop
# Then build:
docker build -f Dockerfile.windows -t huggingface/hf-mount:windows-latest .
```

### 4. Push to Container Registry

```powershell
# Login to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin

# Tag and push
docker tag huggingface/hf-mount:windows-latest ghcr.io/huggingface/hf-mount:windows-latest
docker push ghcr.io/huggingface/hf-mount:windows-latest
```

### 5. Update CSI Manifests

After pushing the image, update the Windows DaemonSet in `deploy/csi/node.yaml`:

```yaml
# Line ~140 in deploy/csi/node.yaml
- name: csi-driver
  image: ghcr.io/huggingface/hf-mount:windows-latest  # Updated from huggingface/hf-mount:windows-latest
  imagePullPolicy: IfNotPresent
```

## Automated Build Script

For convenience, here's a PowerShell script that automates the build process:

```powershell
# build-windows-csi.ps1
$ErrorActionPreference = "Stop"

Write-Host "Building hf-mount Windows CSI driver..." -ForegroundColor Green

# Build the project
Write-Host "Building Rust project..." -ForegroundColor Yellow
cargo build --release --features nfs,vendored-openssl --bin hf-mount-csi

# Check if build succeeded
if (-not (Test-Path "target\release\hf-mount-csi.exe")) {
    Write-Error "Build failed - hf-mount-csi.exe not found"
    exit 1
}

Write-Host "Build successful!" -ForegroundColor Green
Write-Host "Binary location: target\release\hf-mount-csi.exe" -ForegroundColor Cyan

# Build Docker image if Docker is available
if (Get-Command docker -ErrorAction SilentlyContinue) {
    Write-Host "Building Docker image..." -ForegroundColor Yellow
    
    # Create temporary Dockerfile
    $dockerfile = @"
FROM mcr.microsoft.com/windows/servercore:ltsc2022
COPY target/release/hf-mount-csi.exe /hf-mount-csi.exe
ENTRYPOINT ["hf-mount-csi.exe"]
CMD ["--endpoint=0.0.0.0:50051"]
"@
    
    $dockerfile | Out-File -FilePath "Dockerfile.windows.temp" -Encoding ASCII
    
    docker build -f Dockerfile.windows.temp -t huggingface/hf-mount:windows-latest .
    
    # Cleanup
    Remove-Item Dockerfile.windows.temp
    
    Write-Host "Docker image built: huggingface/hf-mount:windows-latest" -ForegroundColor Green
} else {
    Write-Warning "Docker not found - skipping image build"
}

Write-Host "Build complete!" -ForegroundColor Green
```

Run the script:
```powershell
.\build-windows-csi.ps1
```

## Verification

Test the built image locally:

```powershell
# Test the binary directly
.\target\release\hf-mount-csi.exe --help

# Test the Docker container
docker run --rm huggingface/hf-mount:windows-latest --help
```

## Troubleshooting

**Issue: "linker `link.exe` not found"**
- Solution: Install Microsoft Visual Studio Build Tools or use the GNU toolchain with `x86_64-pc-windows-gnu` target

**Issue: Docker build fails on Windows**
- Solution: Ensure Docker Desktop is set to use Windows containers (not Linux containers)

**Issue: "failed to run custom build command for `openssl-sys`"**
- Solution: Use `--features vendored-openssl` to bundle OpenSSL statically

**Issue: Container too large**
- Solution: Use a smaller base image like `mcr.microsoft.com/windows/nanoserver:ltsc2022` if your application doesn't need full Server Core

## Future Automation

To automate Windows builds in CI, the following would need to be added to `.github/workflows/release.yml`:

1. Windows self-hosted runner or GitHub Actions Windows runner
2. Windows container build step
3. Push step to container registry
4. Update of CSI manifests with new image tag

This is planned for a future release after the alpha phase.