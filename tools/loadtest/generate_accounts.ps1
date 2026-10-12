param(
    [ValidateRange(1, 1000000)]
    [int]$Count = 5000,

    [string]$OutputPath = (Join-Path $PSScriptRoot 'accounts.csv')
)

$lines = [System.Collections.Generic.List[string]]::new($Count + 1)
$lines.Add('email,password,peer_uid,thread_id')

for ($i = 1; $i -le $Count; $i++) {
    $email = 'loaduser{0:D6}@example.com' -f $i
    $lines.Add("$email,123456,,")
}

$absoluteOutputPath = [System.IO.Path]::GetFullPath($OutputPath)
$encoding = [System.Text.UTF8Encoding]::new($false)
[System.IO.File]::WriteAllLines($absoluteOutputPath, $lines, $encoding)

Write-Host "Generated $Count accounts: $absoluteOutputPath"
