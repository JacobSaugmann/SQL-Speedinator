# Test alternative PowerShell Get-Counter paths
Write-Host "Testing alternative performance counter paths..."

# Test with English names
$englishCounters = @(
    "\Processor(_Total)\% Processor Time",
    "\System\Processor Queue Length",
    "\Memory\Available MBytes",
    "\Memory\Pages/sec"
)

# Test with localized discovery
try {
    Write-Host "Discovering available counters..."
    $allCounters = Get-Counter -ListSet "*" | Select-Object -First 5
    foreach ($set in $allCounters) {
        Write-Host "Counter Set: $($set.CounterSetName)"
        $set.Paths | Select-Object -First 2 | ForEach-Object {
            Write-Host "  - $_"
        }
    }
} catch {
    Write-Host "Error listing counter sets: $($_.Exception.Message)"
}

# Try a simple test counter
try {
    Write-Host "Testing simple processor counter..."
    $sample = Get-Counter -Counter "\Processor Information(_Total)\% Processor Time" -SampleInterval 1 -MaxSamples 1 -ErrorAction Stop
    Write-Host "Success with Processor Information path!"
    Write-Host "Value: $($sample.CounterSamples[0].CookedValue)"
} catch {
    Write-Host "Failed with Processor Information: $($_.Exception.Message)"
}

# Try with WMI instead
try {
    Write-Host "Testing with WMI as fallback..."
    $cpu = Get-WmiObject -Class Win32_Processor | Measure-Object -Property LoadPercentage -Average
    $memory = Get-WmiObject -Class Win32_OperatingSystem
    Write-Host "WMI CPU Average: $($cpu.Average)%"
    Write-Host "WMI Memory Available: $([math]::Round($memory.FreePhysicalMemory/1024, 0)) MB"
} catch {
    Write-Host "WMI also failed: $($_.Exception.Message)"
}