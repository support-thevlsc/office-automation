# ============================================================
# VLSC DocumentHub – Master Bootstrap Script (Object-based)
# ------------------------------------------------------------
# 1) Rebuilds folder/file structure from ZIP
# 2) Generates Power Automate flow JSON templates:
#      - Books -> Support
#      - Support -> Admin
#      - Admin -> Owner
# ============================================================

param(
    # --- Core paths ---
    [Parameter(Mandatory = $true)]
    [string]$ZipPath,

    [Parameter(Mandatory = $false)]
    [string]$BasePath,

    # --- Optional: script to run after everything completes ---
    [Parameter(Mandatory = $false)]
    [string]$NextScriptPath,

    # --- Power Automate / tenant info (for metadata in JSON) ---
    [Parameter(Mandatory = $true)]
    [string]$TenantName,          # e.g. "thevlsc.onmicrosoft.com"

    [Parameter(Mandatory = $true)]
    [string]$EnvironmentName,     # e.g. "Default-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

    # --- Mailboxes ---
    [Parameter(Mandatory = $false)]
    [string]$BooksEmail,

    [Parameter(Mandatory = $false)]
    [string]$SupportEmail,

    [Parameter(Mandatory = $false)]
    [string]$AdminEmail,

    [Parameter(Mandatory = $false)]
    [string]$OwnerEmail,

    # --- SharePoint / libraries ---
    [Parameter(Mandatory = $true)]
    [string]$DocumentHubSiteUrl,  # e.g. "https://thevlsc.sharepoint.com/sites/VLSC-DocHub"

    [Parameter(Mandatory = $false)]
    [string]$WorkingLibraryName,

    [Parameter(Mandatory = $false)]
    [string]$SecureLibraryName
)

$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# Defaults for optional params
# ------------------------------------------------------------
if (-not $BasePath) {
    $BasePath = "C:\Users\mchri\OneDrive - The VLSC\The_VLSC_DocumentHub"
}
if (-not $BooksEmail) {
    $BooksEmail = "books@thevlsc.com"
}
if (-not $SupportEmail) {
    $SupportEmail = "support@thevlsc.com"
}
if (-not $AdminEmail) {
    $AdminEmail = "admin@thevlsc.com"
}
if (-not $OwnerEmail) {
    $OwnerEmail = "christian@thevlsc.com"
}
if (-not $WorkingLibraryName) {
    $WorkingLibraryName = "WorkingDocs"
}
if (-not $SecureLibraryName) {
    $SecureLibraryName  = "The_VLSC_Secure"
}

# Normalize site URL (no trailing slash)
if ($DocumentHubSiteUrl.EndsWith("/")) {
    $DocumentHubSiteUrl = $DocumentHubSiteUrl.TrimEnd("/")
}

Write-Host "=== VLSC DocumentHub – Master Bootstrap ===" -ForegroundColor Cyan
Write-Host "Base path: $BasePath" -ForegroundColor DarkCyan
Write-Host "ZIP path:  $ZipPath" -ForegroundColor DarkCyan

# ------------------------------------------------------------
# Function: Rebuild folders/files from ZIP
# ------------------------------------------------------------
function Invoke-DocumentHubRebuild {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ZipPath,

        [Parameter(Mandatory = $true)]
        [string]$OutputRoot
    )

    if (-not (Test-Path $ZipPath)) {
        throw "ZIP file not found: $ZipPath"
    }

    if (-not (Test-Path $OutputRoot)) {
        Write-Host "Creating base folder: $OutputRoot" -ForegroundColor Yellow
        New-Item -ItemType Directory -Path $OutputRoot | Out-Null
    }

    Write-Host "`n[1/3] Reading ZIP and rebuilding structure..." -ForegroundColor Green

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead($ZipPath)

    foreach ($entry in $zip.Entries) {

        # Normalize paths (ZIP uses /)
        $relativePath = $entry.FullName.Replace('/', '\')

        # Skip empty entries
        if ([string]::IsNullOrWhiteSpace($relativePath)) {
            continue
        }

        $targetPath = Join-Path $OutputRoot $relativePath

        if ($entry.FullName.EndsWith("/")) {
            # Folder entry
            if (-not (Test-Path $targetPath)) {
                Write-Host "Creating folder: $targetPath" -ForegroundColor Green
                New-Item -ItemType Directory -Path $targetPath | Out-Null
            }
        }
        else {
            # File entry
            $targetFolder = Split-Path $targetPath -Parent
            if (-not (Test-Path $targetFolder)) {
                Write-Host "Creating missing folder: $targetFolder" -ForegroundColor Yellow
                New-Item -ItemType Directory -Path $targetFolder | Out-Null
            }

            if (-not (Test-Path $targetPath)) {
                Write-Host "Restoring file: $relativePath" -ForegroundColor Green
                [System.IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $targetPath, $true)
            }
            else {
                Write-Host "Skipping existing file: $relativePath" -ForegroundColor DarkGray
            }
        }
    }

    $zip.Dispose()
    Write-Host "`n[1/3] Rebuild complete." -ForegroundColor Cyan
}

# ------------------------------------------------------------
# Function: Generate Power Automate flow JSON templates
#          (Books -> Support -> Admin -> Owner)
# ------------------------------------------------------------
function New-DocumentHubFlowTemplates {
    param(
        [Parameter(Mandatory = $true)]
        [string]$OutputRoot,

        [Parameter(Mandatory = $true)]
        [string]$TenantName,

        [Parameter(Mandatory = $true)]
        [string]$EnvironmentName,

        [Parameter(Mandatory = $true)]
        [string]$BooksEmail,

        [Parameter(Mandatory = $true)]
        [string]$SupportEmail,

        [Parameter(Mandatory = $true)]
        [string]$AdminEmail,

        [Parameter(Mandatory = $true)]
        [string]$OwnerEmail,

        [Parameter(Mandatory = $true)]
        [string]$DocumentHubSiteUrl,

        [Parameter(Mandatory = $true)]
        [string]$WorkingLibraryName,

        [Parameter(Mandatory = $true)]
        [string]$SecureLibraryName
    )

    Write-Host "`n[2/3] Generating flow JSON templates..." -ForegroundColor Green

    $flowsRoot = Join-Path $OutputRoot "_flows"
    if (-not (Test-Path $flowsRoot)) {
        New-Item -ItemType Directory -Path $flowsRoot | Out-Null
    }

    $booksToSupportPath = Join-Path $flowsRoot "Flow_BooksToSupport.json"
    $supportToAdminPath = Join-Path $flowsRoot "Flow_SupportToAdmin.json"
    $adminToOwnerPath   = Join-Path $flowsRoot "Flow_AdminToOwner.json"

    # ------------------------------
    # 1) Books -> Support template
    # ------------------------------
    $booksObj = [ordered]@{
        displayName = "VLSC - Books to Support Intake"
        description = "Normalize emails sent to books@ and forward to support@ while saving attachments into WorkingDocs/Inbound/Books."
        tenant      = $TenantName
        environment = $EnvironmentName
        mail        = [ordered]@{
            from = $BooksEmail
            to   = $SupportEmail
        }
        sharePoint  = [ordered]@{
            siteUrl = $DocumentHubSiteUrl
            library = $WorkingLibraryName
            folder  = "Inbound/Books"
        }
        notes       = "In Power Automate: trigger on new email to books@, save attachments to the specified library/folder, then forward a summary to support@."
    }

    $booksJson = $booksObj | ConvertTo-Json -Depth 10
    Set-Content -Path $booksToSupportPath -Value $booksJson -Encoding UTF8

    # ------------------------------
    # 2) Support -> Admin template
    # ------------------------------
    $supportObj = [ordered]@{
        displayName = "VLSC - Support to Admin Triage"
        description = "Classify incoming emails to support@ as Sensitive/Financial vs Other, then forward all to admin@ with flags in the subject."
        tenant      = $TenantName
        environment = $EnvironmentName
        mail        = [ordered]@{
            from = $SupportEmail
            to   = $AdminEmail
        }
        rules       = @(
            [ordered]@{
                name                 = "Sensitive or ID"
                matchOnSubjectContains = @("ssn", "passport", "driver license")
                subjectSuffix        = "[SENSITIVE]"
            },
            [ordered]@{
                name                 = "Financial"
                matchOnSubjectContains = @("invoice", "payment", "accounts payable", "accounts receivable")
                subjectSuffix        = "[FINANCIAL]"
            },
            [ordered]@{
                name                 = "Other"
                matchOnSubjectContains = @()
                subjectSuffix        = "[SUPPORT-TRIAGED]"
            }
        )
        notes = "In Power Automate: trigger on new email to support@, add conditions for each rule above, and forward to admin@ with the matching subject suffix."
    }

    $supportJson = $supportObj | ConvertTo-Json -Depth 10
    Set-Content -Path $supportToAdminPath -Value $supportJson -Encoding UTF8

    # ------------------------------
    # 3) Admin -> Owner template
    # ------------------------------
    $adminObj = [ordered]@{
        displayName = "VLSC - Admin to Owner Final Filter"
        description = "From admin@, forward only Sensitive/Financial items to christian@ and archive other items to WorkingDocs/AdminArchive. Save sensitive attachments to The_VLSC_Secure."
        tenant      = $TenantName
        environment = $EnvironmentName
        mail        = [ordered]@{
            from    = $AdminEmail
            toOwner = $OwnerEmail
        }
        sharePointSecure = [ordered]@{
            siteUrl = $DocumentHubSiteUrl
            library = $SecureLibraryName
            folder  = "Inbound/Admin"
        }
        sharePointArchive = [ordered]@{
            siteUrl = $DocumentHubSiteUrl
            library = $WorkingLibraryName
            folder  = "AdminArchive"
        }
        routingLogic = [ordered]@{
            forwardToOwnerIfSubjectContains = @("[SENSITIVE]", "[FINANCIAL]", "SENSITIVE/FINANCIAL")
            elseArchiveOnly                 = $true
        }
        notes = "In Power Automate: trigger on new email to admin@. If subject contains one of the flags, save attachments to the secure library/folder and forward to the Owner; otherwise log/archive to WorkingDocs/AdminArchive."
    }

    $adminJson = $adminObj | ConvertTo-Json -Depth 10
    Set-Content -Path $adminToOwnerPath -Value $adminJson -Encoding UTF8

    Write-Host "[2/3] Flow templates created at: $flowsRoot" -ForegroundColor Cyan
}

# ============================================================
# MAIN EXECUTION
# ============================================================

# Ensure base folder exists
if (-not (Test-Path $BasePath)) {
    Write-Host "Creating base folder: $BasePath" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $BasePath | Out-Null
}

# 1) Rebuild from ZIP
Invoke-DocumentHubRebuild -ZipPath $ZipPath -OutputRoot $BasePath

# 2) Generate flow templates
New-DocumentHubFlowTemplates `
    -OutputRoot $BasePath `
    -TenantName $TenantName `
    -EnvironmentName $EnvironmentName `
    -BooksEmail $BooksEmail `
    -SupportEmail $SupportEmail `
    -AdminEmail $AdminEmail `
    -OwnerEmail $OwnerEmail `
    -DocumentHubSiteUrl $DocumentHubSiteUrl `
    -WorkingLibraryName $WorkingLibraryName `
    -SecureLibraryName $SecureLibraryName

# 3) Optional: run "next script"
if ($NextScriptPath) {
    if (Test-Path $NextScriptPath) {
        Write-Host ""
        Write-Host "[3/3] Running next script: $NextScriptPath" -ForegroundColor Green
        & $NextScriptPath
    }
    else {
        Write-Host ""
        Write-Host "[3/3] NextScriptPath specified but file not found: $NextScriptPath" -ForegroundColor Yellow
    }
}
else {
    Write-Host ""
    Write-Host "[3/3] No next script specified. Master bootstrap complete." -ForegroundColor Cyan
}

Write-Host ""
Write-Host ("DocumentHub rebuilt at: " + $BasePath) -ForegroundColor DarkCyan
Write-Host ("Flow templates located at: " + (Join-Path $BasePath "_flows")) -ForegroundColor DarkCyan
