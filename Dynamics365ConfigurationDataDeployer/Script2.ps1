# N:N
# Create/Update/Upsert
# Business Units, Business Units defaul teams, Currency
# Ability to relay on name while import (defining transformation mapping)
# Owning Teams and Users
# Alias - Enity Ref and OptionSet
# Customer fields


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
    [String[]]$AttributesWithExtendedData
  )

  Write-Output "Exporting records to .CSV by the query from the $FetchXmlQueryFilePath file."
  [string]$fetchXmlText = Get-Content $FetchXmlQueryFilePath -Raw
  $records = (Get-CrmRecordsByFetch -conn $CrmConn -Fetch $fetchXmlText -AllRows).CrmRecords
  
  Write-Output  "$($records.Count) records are being exported..."
  
  #$records
  ##$records | % { Write-Output $_.logicalname }
  
  # if count -eq 0
  $fetchXml = [xml]$fetchXmlText
  [System.Collections.ArrayList]$attributeNames = New-Object System.Collections.ArrayList  
  [System.Collections.ArrayList]$result = New-Object System.Collections.ArrayList
  $header = [pscustomobject]@{}  
  $atts = $fetchXml.GetElementsByTagName('attribute')
  $formattedValueColumnNameSuffix = "!name"
	foreach($att in $atts){
		if($att.ParentNode.HasAttribute('alias')){
			$attName = $att.ParentNode.GetAttribute('alias') + "." + $att.name
		}
		else{
			$attName = $att.name
		}

    $attributeNames.Add($attName)
    Add-Member -InputObject $header -MemberType NoteProperty -Name $attName -Value $attName
    if ($AttributesWithExtendedData -contains $attName) {
      $attributeNames.Add($attName + $formattedValueColumnNameSuffix)
      Add-Member -InputObject $header -MemberType NoteProperty -Name $attName -Value $attName
    }
	}

  foreach($record in $records){
    $row = [pscustomobject]@{}
    foreach($attName in $attributeNames){
      $valuePropName = $attName + "_Property"
      $attProp = $record.$valuePropName
      if ($attProp -eq $null){
        $value = $null
      } else {
        $attPropValue = $attProp.Value
        if ($attPropValue -eq $null){
          $value = $null
        } else {
          if ($attPropValue -is [Microsoft.Xrm.Sdk.AliasedValue]){
            $attPropValue = $attPropValue.Value
          }

          if ($attPropValue.PSobject.Properties.name -match "Value"){
            $value = $attPropValue.Value
          } else {
						if($attPropValue -is [Microsoft.Xrm.Sdk.EntityReference]){
							$value = $attPropValue.Id
						} else {
              $value = $attPropValue
            }
          }
        }
      }
      
      Add-Member -InputObject $row -MemberType NoteProperty -Name $attName -Value $value
      if ($AttributesWithExtendedData -contains $attName) {
        if ($value -ne $null) {
          $value = $record.$attName
        }

        Add-Member -InputObject $row -MemberType NoteProperty -Name ($attName + $formattedValueColumnNameSuffix) -Value $value
      }
    }

    $result.Add($row)
  }
  
  $result | Export-Csv -Path $ExportedFilePath -NoTypeInformation

  Write-Output "The data has been exported into $ExportedFilePath file."
}

try{
  Clear-Host 
  $error.clear()

  $invokationFolder = $PSScriptRoot
  write-output $hst.Name
  #Import-Module ("$invokationFolder\CommonLib.ps1")

  ## ----------------------- UNCOMMENT THIS AS NEEDED TO SETUP SMOOTH CONNECTION -----------------------------
  ## Note: 
  ## You have to store a password hash in a file. You can use \OtherHelpers\CreateSecureStringFile.ps1 
  ## to build its secure content.

  Write-Output "Prepare credentials to connect to Dynamics smoothly (using saved credentials)..."

  $serverUrl = "http://test01"
  $orgName = "org01" # only On-Premise will need it
  $userName = "test\administrator"

  $pathToCred = "$invokationFolder\testcred.txt"
  $pass = Get-Content $pathToCred | ConvertTo-SecureString
  $cred = new-object -typename System.Management.Automation.PSCredential -argumentlist $userName, $pass

  Write-Output "Connecting to Dynamics..."

  # Connecting to On-Premise (note: uncomment one of the two "$conn=" rows below)  
  $conn = Connect-CrmOnPremDiscovery -Credential $cred -ServerUrl $serverUrl -OrganizationName $orgName 

  ## Connecting to On-Online (note: uncomment one of the two "$conn=" rows above/below)  
  #$conn = Connect-CrmOnline -Credential $cred -ServerUrl $serverUrl
  
  ## ---------------------------------------------------------------------------------------------------------
  
  # Connecting to a Dynamics instance/organization using "INTERACTIVE MODE" / WIZARD MODE
  # Note: Comment the row below in case you decided to uncomment the block above to connect smoothly   
  #$conn = Build-CrmConnection -InteractiveMode -Verbose


  Export-EntityData -CrmConn $conn -FetchXmlQueryFilePath "$invokationFolder\Query1.xml" -ExportedFilePath "$invokationFolder\temp\test2.csv"

}
catch [System.Managment.Automation.ActionPreferenceStopException]{
  throw $_.Exception
}

catch [Exception] {
  throw $_.Exception
}