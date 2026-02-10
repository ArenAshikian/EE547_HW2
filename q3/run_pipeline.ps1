<#
run_pipeline.ps1
Windows/PowerShell equivalent of run_pipeline.sh.

Usage:
  .\run_pipeline.ps1 https://www.example.com https://httpbin.org/html
#>

param(
  [Parameter(Mandatory = $true, ValueFromRemainingArguments = $true)]
  [string[]] $Urls
)

Write-Host "Starting Multi-Container Pipeline"
Write-Host "================================="

if ($Urls.Count -lt 1) {
  Write-Host "Usage: .\run_pipeline.ps1 <url1> [url2] [url3] ..."
  Write-Host "Example: .\run_pipeline.ps1 https://example.com https://wikipedia.org"
  exit 1
}

Write-Host "URLs to process:"
$Urls | ForEach-Object { Write-Host $_ }
Write-Host ""

# Clean previous runs (matches bash: docker-compose down -v)
docker compose down -v 2>$null | Out-Null

# Create temporary directory (matches bash mktemp -d)
$tempDir = Join-Path $env:TEMP ("pipe_" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null
$urlsFile = Join-Path $tempDir "urls.txt"

# Create URL list (one URL per line, like bash loop with >>)
# Use ASCII to match typical shell behavior; newline between entries is important.
$Urls | Set-Content -Encoding ascii -Path $urlsFile

try {
  # Build containers (bash uses --quiet; PowerShell doesn't need output)
  Write-Host "Building containers..."
  docker compose build *> $null
  if ($LASTEXITCODE -ne 0) { throw "docker compose build failed" }

  # Start pipeline
  Write-Host "Starting pipeline..."
  docker compose up -d | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "docker compose up -d failed" }

  Start-Sleep -Seconds 3

  # Ensure /shared/input exists inside the shared volume (fetcher expects it)
  # This is the one practical Windows fix you already discovered.
  docker exec pipeline-fetcher sh -lc "mkdir -p /shared/input" 2>$null | Out-Null

  # Inject URLs (matches bash docker cp TEMP_DIR/urls.txt pipeline-fetcher:/shared/input/urls.txt)
  Write-Host "Injecting URLs..."
  docker cp $urlsFile "pipeline-fetcher:/shared/input/urls.txt" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "docker cp urls.txt into pipeline-fetcher failed" }

  # Monitor completion (matches bash loop, but with fallback if analyzer exits)
  Write-Host "Processing..."
  $maxWait = 300
  $elapsed = 0

  while ($elapsed -lt $maxWait) {

    # Try bash-equivalent check first: docker exec pipeline-analyzer test -f ...
    $analyzerRunning = $false
    try {
      $state = docker inspect -f '{{.State.Running}}' pipeline-analyzer 2>$null
      if ($state -eq "true") { $analyzerRunning = $true }
    } catch { $analyzerRunning = $false }

    if ($analyzerRunning) {
      docker exec pipeline-analyzer test -f /shared/analysis/final_report.json 2>$null | Out-Null
      if ($LASTEXITCODE -eq 0) {
        Write-Host "Pipeline complete"
        break
      }
    }
    else {
      # Fallback: container may have exited after writing output; check the volume directly
      docker run --rm -v pipeline-shared-data:/shared alpine sh -lc "test -f /shared/analysis/final_report.json" 2>$null | Out-Null
      if ($LASTEXITCODE -eq 0) {
        Write-Host "Pipeline complete"
        break
      }
    }

    Start-Sleep -Seconds 5
    $elapsed += 5
  }

  if ($elapsed -ge $maxWait) {
    Write-Host "Pipeline timeout after $maxWait seconds"
    docker compose logs | Out-Host
    docker compose down | Out-Null
    exit 1
  }

  # Extract results (matches bash: mkdir -p output; docker cp from analyzer container)
  New-Item -ItemType Directory -Force -Path "output" | Out-Null

  $copiedFromContainer = $false
  try {
    # Prefer bash-equivalent: copy from analyzer container
    docker cp "pipeline-analyzer:/shared/analysis/final_report.json" "output/" 2>$null | Out-Null
    docker cp "pipeline-analyzer:/shared/status" "output/" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $copiedFromContainer = $true }
  } catch { $copiedFromContainer = $false }

  if (-not $copiedFromContainer) {
    # If container is gone, copy from volume (Windows reliability fallback)
    docker run --rm -v pipeline-shared-data:/shared -v "${PWD}:/out" alpine sh -lc "mkdir -p /out/output/status; cp /shared/analysis/final_report.json /out/output/final_report.json; cp -r /shared/status/* /out/output/status/" | Out-Null
  }

  # Cleanup (matches bash docker-compose down)
  docker compose down | Out-Null

  # Display summary (bash: print first 20 lines)
  if (Test-Path "output\final_report.json") {
    Write-Host ""
    Write-Host "Results saved to output\final_report.json"
    Get-Content "output\final_report.json" | Select-Object -First 20
  } else {
    Write-Host "Pipeline failed - no output generated"
    exit 1
  }
}
finally {
  if (Test-Path $tempDir) { Remove-Item -Recurse -Force $tempDir -ErrorAction SilentlyContinue }
}
