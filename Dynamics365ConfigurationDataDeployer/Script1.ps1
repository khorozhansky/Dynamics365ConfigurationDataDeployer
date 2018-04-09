Clear-Host 
$error.clear()

$invokationFolder = $PSScriptRoot

$employees = @(

  [pscustomobject]@{

  FirstName = 'Adam'

  LastName  = 'B"ertram'

  Username  = 'ab\e""rt/ram'

  }

  [pscustomobject]@{

  FirstName = 'Jo#\ne'

  LastName  = 'Jones'

  Username  = 'jjones'

  }

  [pscustomobject]@{

  FirstName = 'Mary'

  LastName  = 'Baker'

  Username  = 'mbaker'

  }

  )

  $employees | Export-Csv -Path "$invokationFolder\temp\test1.csv" -NoTypeInformation