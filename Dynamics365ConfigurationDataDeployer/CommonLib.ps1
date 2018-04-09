# N:N
# Create/Update/Upsert
# Business Units, Business Units defaul teams, Currency

Import-Module Microsoft.Xrm.Data.Powershell

function Validate-CrmConnection {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$False)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn
  )

  if($CrmConn -eq $null)
  {
    throw 'A connection to CRM is not specified.'
  }
}

function Test-SolutionFilePath {
  [CmdletBinding()]
  param(
    [parameter(Mandatory=$True)]
    [string]$Path
  )

  if(-not (Test-Path $Path)){
    throw [System.IO.FileNotFoundException] "Solution file ('$Path') not found."
  }
}

function Export-EntityData {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$FetchXmlQueryFilePath,
    [Parameter(Mandatory=$True)]
    [String]$ExportedFilePath,
    [Parameter(Mandatory=$False)]
    [String[]]$AttributesWithFormattedValues
  )

  [string]$fetchXml = Get-Content $FetchXmlQueryFilePath -Raw
  $records = Get-CrmRecordsByFetch -conn $CrmConn -Fetch $fetchXml -AllRows
  $record = $records[0]
  $stateCode = $record.statecode
  #Export-Csv -Path $ExportedFilePath
}

