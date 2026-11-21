
# ======================================================
# E-DF PLATFORM - WINDOWS STARTUP SCRIPT
# Nodes of Glory
#
# Starts:
#   1. Kafka
#   2. FastAPI
#   3. Streamlit
#
# Spark, Producers and Predictors start through
# station activation in the dashboard.
# ======================================================

$ErrorActionPreference = "Stop"

# ======================================================
# 1. PROJECT CONFIGURATION
# ======================================================

# Automatically use the folder containing this script
$Project = $PSScriptRoot

$Python = "C:\Users\hp\Anaconda3\envs\electricity-spark\python.exe"

$KafkaHome = "C:\kafka_2.13-4.3.1"

$JavaHome = "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"

$KafkaPort = 9092
$ApiPort = 8000
$DashboardPort = 8501

$DashboardUrl = "http://127.0.0.1:$DashboardPort"

# ======================================================
# 2. ENVIRONMENT CONFIGURATION
# ======================================================

$env:JAVA_HOME = $JavaHome
$env:PYTHONPATH = $Project
$env:PYSPARK_PYTHON = $Python

$env:PATH = "$JavaHome\bin;$env:PATH"

# ======================================================
# 3. AUTOMATIC HADOOP DETECTION
# ======================================================

Write-Host ""
Write-Host "Checking Hadoop environment..." -ForegroundColor Cyan

# Check existing HADOOP_HOME first
$HadoopHome = $env:HADOOP_HOME

# Try common installation directories
$HadoopCandidates = @(
    "C:\hadoop-3.5.0",
    "C:\hadoop",
    "C:\hadoop-3.4.0",
    "C:\hadoop-3.3.6"
)

if (-not $HadoopHome -or -not (Test-Path $HadoopHome)) {

    $HadoopHome = $null

    foreach ($Candidate in $HadoopCandidates) {

        if (Test-Path $Candidate) {

            $HadoopHome = $Candidate
            break

        }
    }
}

if ($HadoopHome -and (Test-Path $HadoopHome)) {

    $env:HADOOP_HOME = $HadoopHome

    $HadoopBin = Join-Path $HadoopHome "bin"

    if (Test-Path $HadoopBin) {
        $env:PATH = "$HadoopBin;$env:PATH"
    }

    Write-Host "Hadoop detected: $HadoopHome" `
        -ForegroundColor Green

}
else {

    # Clear an invalid inherited HADOOP_HOME
    Remove-Item Env:HADOOP_HOME `
        -ErrorAction SilentlyContinue

    Write-Host "Hadoop installation not detected." `
        -ForegroundColor Yellow

    Write-Host "Dashboard startup will continue." `
        -ForegroundColor Yellow

    Write-Host "Spark may need Hadoop configured later." `
        -ForegroundColor Yellow
}

# ======================================================
# 4. HELPER FUNCTIONS
# ======================================================

function Test-Port {

    param(
        [int]$Port
    )

    $tcp = New-Object System.Net.Sockets.TcpClient

    try {

        $tcp.Connect("127.0.0.1", $Port)

        return $tcp.Connected

    }
    catch {

        return $false

    }
    finally {

        $tcp.Close()

    }
}

function Wait-ForPort {

    param(
        [int]$Port,
        [int]$TimeoutSeconds = 90,
        [System.Diagnostics.Process]$Process = $null
    )

    $watch = [System.Diagnostics.Stopwatch]::StartNew()

    while ($watch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {

        if (Test-Port $Port) {
            return $true
        }

        if ($null -ne $Process) {

            $Process.Refresh()

            if ($Process.HasExited) {
                return $false
            }
        }

        Start-Sleep -Seconds 2
    }

    return $false
}

function Show-ErrorLog {

    param(
        [string]$File
    )

    if (Test-Path $File) {

        Write-Host ""
        Write-Host "--- Recent Error Log ---" `
            -ForegroundColor Red

        Get-Content $File -Tail 50

    }
}

# ======================================================
# 5. BROWSER LAUNCH
# ======================================================

function Open-EDFDashboard {

    param(
        [string]$Url
    )

    Write-Host ""
    Write-Host "Opening E-DF Dashboard..." `
        -ForegroundColor Cyan

    $Browsers = @(
        "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe",
        "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    )

    foreach ($Browser in $Browsers) {

        if (Test-Path $Browser) {

            try {

                Start-Process `
                    -FilePath $Browser `
                    -ArgumentList $Url `
                    -ErrorAction Stop

                Write-Host "Browser launch requested." `
                    -ForegroundColor Green

                return

            }
            catch {

                Write-Host "Browser launch failed: $Browser" `
                    -ForegroundColor Yellow

            }
        }
    }

    Write-Host "Open the dashboard manually:" `
        -ForegroundColor Yellow

    Write-Host $Url -ForegroundColor Cyan
}

# ======================================================
# 6. VALIDATE REQUIRED FILES
# ======================================================

$RequiredPaths = @(
    $Project,
    $Python,
    $KafkaHome,
    $JavaHome,
    (Join-Path $Project "backend\main.py"),
    (Join-Path $Project "dashboard\app.py")
)

foreach ($Path in $RequiredPaths) {

    if (-not (Test-Path $Path)) {

        throw "Required path not found: $Path"

    }
}

# Hadoop is optional for dashboard startup.
# It is NOT included in mandatory path validation.

$LogDir = Join-Path $Project "logs"

New-Item `
    -ItemType Directory `
    -Force `
    -Path $LogDir | Out-Null

Set-Location $Project

Write-Host ""
Write-Host "====================================" `
    -ForegroundColor Cyan

Write-Host "       E-DF PLATFORM STARTUP" `
    -ForegroundColor Cyan

Write-Host "====================================" `
    -ForegroundColor Cyan

Write-Host ""

# ======================================================
# STEP 1 - KAFKA
# ======================================================

Write-Host "[1/3] Checking Kafka..." `
    -ForegroundColor Yellow

if (Test-Port $KafkaPort) {

    Write-Host "Kafka already running." `
        -ForegroundColor Green

}
else {

    $KafkaStart = Join-Path `
        $KafkaHome "bin\windows\kafka-server-start.bat"

    $KafkaConfig = Join-Path `
        $KafkaHome "config\server.properties"

    if (-not (Test-Path $KafkaConfig)) {

        $KafkaConfig = Join-Path `
            $KafkaHome "config\kraft\server.properties"

    }

    if (-not (Test-Path $KafkaStart)) {
        throw "Kafka startup script not found."
    }

    if (-not (Test-Path $KafkaConfig)) {
        throw "Kafka configuration not found."
    }

    Write-Host "Starting Kafka..." `
        -ForegroundColor Cyan

    $KafkaCommand = 'call "{0}" "{1}"' -f `
        $KafkaStart, $KafkaConfig

    Start-Process `
        -FilePath "cmd.exe" `
        -ArgumentList @("/k", $KafkaCommand) `
        -WorkingDirectory $KafkaHome

    $KafkaReady = Wait-ForPort `
        -Port $KafkaPort `
        -TimeoutSeconds 90

    if (-not $KafkaReady) {

        throw "Kafka failed to start. Check Kafka console."

    }

    Write-Host "Kafka started successfully." `
        -ForegroundColor Green
}

# ======================================================
# STEP 2 - FASTAPI
# ======================================================

Write-Host ""
Write-Host "[2/3] Checking FastAPI..." `
    -ForegroundColor Yellow

$ApiOutLog = Join-Path `
    $LogDir "fastapi_startup.out.log"

$ApiErrLog = Join-Path `
    $LogDir "fastapi_startup.err.log"

if (Test-Port $ApiPort) {

    Write-Host "FastAPI port already active." `
        -ForegroundColor Green

}
else {

    Write-Host "Starting FastAPI..." `
        -ForegroundColor Cyan

    $ApiProcess = Start-Process `
        -FilePath $Python `
        -ArgumentList @(
            "-m",
            "uvicorn",
            "backend.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "$ApiPort",
            "--log-level",
            "info"
        ) `
        -WorkingDirectory $Project `
        -RedirectStandardOutput $ApiOutLog `
        -RedirectStandardError $ApiErrLog `
        -PassThru

    $ApiReady = Wait-ForPort `
        -Port $ApiPort `
        -TimeoutSeconds 90 `
        -Process $ApiProcess

    if (-not $ApiReady) {

        Write-Host "FastAPI startup failed." `
            -ForegroundColor Red

        Show-ErrorLog $ApiErrLog

        throw "FastAPI failed. Check logs folder."

    }

    Write-Host "FastAPI started successfully." `
        -ForegroundColor Green
}

# ======================================================
# STEP 3 - STREAMLIT
# ======================================================

Write-Host ""
Write-Host "[3/3] Checking Streamlit..." `
    -ForegroundColor Yellow

$StreamlitOutLog = Join-Path `
    $LogDir "streamlit_startup.out.log"

$StreamlitErrLog = Join-Path `
    $LogDir "streamlit_startup.err.log"

if (Test-Port $DashboardPort) {

    Write-Host "Streamlit already running." `
        -ForegroundColor Green

}
else {

    Write-Host "Starting Streamlit..." `
        -ForegroundColor Cyan

    $StreamlitProcess = Start-Process `
        -FilePath $Python `
        -ArgumentList @(
            "-m",
            "streamlit",
            "run",
            "dashboard/app.py",
            "--server.address",
            "127.0.0.1",
            "--server.port",
            "$DashboardPort",
            "--server.headless",
            "true"
        ) `
        -WorkingDirectory $Project `
        -RedirectStandardOutput $StreamlitOutLog `
        -RedirectStandardError $StreamlitErrLog `
        -PassThru

    $DashboardReady = Wait-ForPort `
        -Port $DashboardPort `
        -TimeoutSeconds 90 `
        -Process $StreamlitProcess

    if (-not $DashboardReady) {

        Write-Host "Streamlit startup failed." `
            -ForegroundColor Red

        Show-ErrorLog $StreamlitErrLog

        throw "Streamlit failed. Check logs folder."

    }

    Write-Host "Streamlit started successfully." `
        -ForegroundColor Green
}

# ======================================================
# STARTUP COMPLETE
# ======================================================

Write-Host ""
Write-Host "====================================" `
    -ForegroundColor Green

Write-Host "       E-DF PLATFORM STARTED" `
    -ForegroundColor Green

Write-Host "====================================" `
    -ForegroundColor Green

Write-Host ""
Write-Host "Kafka     : 127.0.0.1:9092"
Write-Host "FastAPI   : http://127.0.0.1:8000/docs"
Write-Host "Dashboard : http://127.0.0.1:8501"
Write-Host ""

Open-EDFDashboard -Url $DashboardUrl

Write-Host ""
Write-Host "------------------------------------" `
    -ForegroundColor Green

Write-Host "Startup sequence completed." `
    -ForegroundColor Green

Write-Host "Activate stations from Home dashboard." `
    -ForegroundColor Yellow

Write-Host "------------------------------------" `
    -ForegroundColor Green

Write-Host ""
