# inspect-oracle-migration-artifacts.ps1
$ErrorActionPreference = "Stop"

$env:AWS_PAGER = ""
$Region = "us-east-1"
$Bucket = "oracle-migration-artifacts-448792658038"

Write-Host "=== AWS Identity ==="
aws sts get-caller-identity

Write-Host "`n=== Region ==="
aws configure get region

Write-Host "`n=== Bucket ==="
"s3://$Bucket"

Write-Host "`n=== Top-level prefixes (first 50) ==="
aws s3api list-objects-v2 --bucket $Bucket --delimiter "/" --max-keys 50 `
  --query "CommonPrefixes[].Prefix" --output table

function Get-AllS3Objects {
  param([string]$BucketName)

  $token = $null
  $all = New-Object System.Collections.Generic.List[object]

  while ($true) {
    $args = @("s3api","list-objects-v2","--bucket",$BucketName,"--max-keys","1000","--output","json")
    if ($token) { $args += @("--continuation-token",$token) }

    $resp = (& aws @args) | ConvertFrom-Json
    if ($resp.Contents) { foreach ($o in $resp.Contents) { $all.Add($o) } }

    if (-not $resp.IsTruncated) { break }
    $token = $resp.NextContinuationToken
  }

  return $all
}

Write-Host "`n=== Searching for metrics.json (bucket-wide) ==="
$objs = Get-AllS3Objects -BucketName $Bucket

$metrics = $objs |
  Where-Object { $_.Key -like "*metrics.json" } |
  Select-Object Key, Size, LastModified |
  Sort-Object { [datetime]$_.LastModified } -Descending

if (-not $metrics -or $metrics.Count -eq 0) { throw "No metrics.json found in s3://$Bucket" }

Write-Host "`nTop 10 metrics.json (most recent first):"
$metrics | Select-Object -First 10 | Format-Table -AutoSize

$SelectedMetricsKey = $metrics[0].Key
Write-Host "`nSelected metrics.json:"
$SelectedMetricsKey

if ($SelectedMetricsKey -match "/raw/") {
  $RunPrefix = ($SelectedMetricsKey -split "/raw/")[0] + "/"
} else {
  $lastSlash = $SelectedMetricsKey.LastIndexOf("/")
  $RunPrefix = if ($lastSlash -ge 0) { $SelectedMetricsKey.Substring(0, $lastSlash + 1) } else { "" }
}

Write-Host "`nDerived run prefix:"
$RunPrefix

Write-Host "`nFirst 50 objects under run prefix:"
aws s3api list-objects-v2 --bucket $Bucket --prefix $RunPrefix --max-keys 200 `
  --query "Contents[:50].[Key,Size,LastModified]" --output table

Write-Host "`nDownloading selected metrics.json to .\metrics.json"
aws s3 cp ("s3://{0}/{1}" -f $Bucket, $SelectedMetricsKey) .\metrics.json | Out-Null

Write-Host "`nmetrics.json top-level keys:"
$mx = Get-Content .\metrics.json -Raw | ConvertFrom-Json
($mx | Get-Member -MemberType NoteProperty).Name | ForEach-Object { " - $_" }

Write-Host "`nBedrock Claude models (first 20) in us-east-1:"
aws bedrock list-foundation-models --region $Region `
  --query "(modelSummaries[?contains(modelId,'claude')].[modelId,providerName,modelName])[:20]" `
  --output table
