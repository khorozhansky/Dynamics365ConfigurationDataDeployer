try {
  Clear-Host 
  $error.clear()

  Import-Module Microsoft.Xrm.Data.Powershell

  $invokationFolder = $PSScriptRoot
  Write-Output "Prepare credentials to connect to Dynamics smoothly (using saved credentials)..."
  $serverUrl = "http://test01"
  $orgName = "org04" # only On-Premise will need it
  $userName = "test\administrator"
  $pathToCred = "$invokationFolder\testcred.txt"
  $pass = Get-Content $pathToCred | ConvertTo-SecureString
  $cred = new-object -typename System.Management.Automation.PSCredential -argumentlist $userName, $pass
  Write-Output "Connecting to Dynamics..."
  # Connecting to On-Premise (note: uncomment one of the two "$conn=" rows below)  
  $conn = Connect-CrmOnPremDiscovery -Credential $cred -ServerUrl $serverUrl -OrganizationName $orgName 

  [Microsoft.Xrm.Sdk.Entity] $record = New-Object Microsoft.Xrm.Sdk.Entity -ArgumentList("account")
  [Microsoft.Xrm.Sdk.AttributeCollection] $attrs = New-Object Microsoft.Xrm.Sdk.AttributeCollection
  $accountIdAttrValue = [Guid]::Parse("c9ce22fe-fa69-e511-80ce-080027111fb1")
  $attrs.Add("accountid", $accountIdAttrValue)
  
  $record.Id = $accountIdAttrValue

  $nameAttrValue = "Adventure Works (sample) 2 "  
  $attrs.Add("name", $nameAttrValue)


  $telephone1AttrValue = "34321432104324-11"  
  $attrs.Add("telephone1", $telephone1AttrValue)


  $shippingmethodcodeAttrValue = New-Object -TypeName Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList @(1)
  $attrs.Add("shippingmethodcode", $shippingmethodcodeAttrValue)

  #$openrevenueAttrValue = New-Object Microsoft.Xrm.Sdk.Money
  #$openrevenueAttrValue.Value = [decimal]::Parse("8933211")
  #$attrs.Add("openrevenue", $openrevenueAttrValue)


  #$opendealsAttrValue = [Int32]::Parse("3")
  #$attrs.Add("opendeals", 2)
  
  $attrs.Add("statecode", (New-Object -TypeName Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList @([Int32]1)))

  $attrs.Add("statuscode", (New-Object -TypeName Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList @([Int32]2)))


  $record.Attributes = $attrs
  
  $upsertRequest = New-Object -TypeName Microsoft.Xrm.Sdk.Messages.UpsertRequest 
  $upsertRequest.Target = $record
  
  $resposne = $conn.Execute($upsertRequest) ;

  #$conn.UpdateStateAndStatusForEntity("account", $accountIdAttrValue, "Inactive", "Inactive")

  #$r = New-Object -TypeName Microsoft.Xrm.Sdk.Messages.SetStateRequest
  #$r.State = New-Object -TypeName Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList @(1)
  #$r.Status = New-Object -TypeName Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList @(2)
  #$r.EntityMoniker = New-Object -TypeName Microsoft.Xrm.Sdk.EntityReference -ArgumentList @("account",$accountIdAttrValue)
  #$resposne = $conn.Execute($r) ;















  #$a = @{}
  #$a.Add("K1", "V1")
  #$a.Add("K2", 1312)
  #$a.Add("K3", 32.23)
  #$a.Add("K4", $null)
  #$a.Add("K5", $false)

  #foreach($k in $a.GetEnumerator()) {
  #  Write-Output "$($k.Key) - $($k.Value)"
  #} 



  Write-Output "Done"
}

catch [Exception] {
  throw $_.Exception
}