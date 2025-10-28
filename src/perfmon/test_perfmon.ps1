# Test PowerShell Get-Counter
$counters = @(
    "\Processor(_Total)\% Processor Time",
    "\System\Processor Queue Length",
    "\Memory\Available MBytes",
    "\Memory\Pages/sec"
)

Write-Host "Testing performance counters..."

try {
    $sample = Get-Counter -Counter $counters -SampleInterval 1 -MaxSamples 1 -ErrorAction Stop
    Write-Host "Success! Got sample at: $($sample.Timestamp)"
    
    foreach ($counter in $sample.CounterSamples) {
        Write-Host "  $($counter.Path): $($counter.CookedValue)"
    }
} catch {
    Write-Host "Error getting counters: $($_.Exception.Message)"
    
    # Try individual counters
    foreach ($counter in $counters) {
        try {
            $individual = Get-Counter -Counter $counter -SampleInterval 1 -MaxSamples 1 -ErrorAction Stop
            Write-Host "  Individual counter '$counter' works"
        } catch {
            Write-Host "  Individual counter '$counter' failed: $($_.Exception.Message)"
        }
    }
}