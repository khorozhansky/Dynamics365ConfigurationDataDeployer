# N:N
# Create/Update/Upsert
# [Done] Business Units (how to process hierarchy) 
# [Done] Currency
# Business Units defaul teams, 
# Ability to relay on name while import (defining transformation mapping)
# [Test Team] (In Progress) Owning Teams and Users
# [Done] Alias - Entity Ref and OptionSet
# [Test] Customer fields 
# [Done] CreatedOn, CreatedBy, ModifiedOn, ModifiedBy - what is possible with create mode only
# Upsert
# [In Progress] Teams
# Default Queues for Teams
# Team Templates (plus related attribute in teams)
# Field security profile
# Update to null
# Details of exceptions
# It's not intended to transfer activity entities
# SearchText - try to do more flexible (not string only)
# Entity Name field
# EntityName field type

# [Test ML Members]

# ! set customer field in a separate action (can refer back to the contacts as well)

# Teams
# Teams' roles
# Teams' field profiles

# Method descriptions

# Possible issues: createdon or createdby specified and expected to update records. TODO: maybe find the duplicate issue and use upsert for this case.

# Import sample data
# Import data maps

<# 
TODO: 
1. Disable / Activate Business Units (now changing "isdisabled" doesn't work (at least in 2016 Update 1))
2. Lookup in active only
#>


# NOTES: 
# 1. Never set State / Status reason during creation (issue with setting Inactive state for new records)
# 2. Business Unit names, Team names, Queue Name(?) and User' full names must be unique. For some entities (not all?) like User you can use custom mapping for owner field (for example username field for system user)
# 3. Self referenced entities - order of export - import (Usually do not import ref to parent on the first stage. Exception: Business Units)
# 4. Deactivate Plugin Steps, Workflows, Business Rules and BPF(?)
# 5. ! Create new Full Business Unit Path calculated field

# QUESTIONS:
# 1. What to do with connection cache in case we change target organization (need close/open VS to get around this)


<#

- User Primary Field
- Exclude inactive
- Specify field to lookup

#>


Import-Module Microsoft.Xrm.Data.Powershell

Set-Variable -Name ImportCache -Scope global -Value ([hashtable]@{ })

function Get-IdValueFromCache {
  [CmdletBinding()]
  param (
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$EntityName,
    [Parameter(Mandatory=$True)]
    [String]$PrimaryKeyField,
    [Parameter(Mandatory=$True)]
    [String]$SearchField,
    [Parameter(Mandatory=$True)]
    [String]$SearchText
  )
  
  $cachedRecordSetName = "$EntityName-$SearchField"

  if (-not $global:ImportCache.ContainsKey($cachedRecordSetName)) {
    $global:ImportCache.Add($cachedRecordSetName, @{ Records = [hashtable]@{} })
  }
  
  $cachedRecords = $global:ImportCache.$cachedRecordSetName.Records
  $cachedId = $cachedRecords.Item($SearchText)
  if ($cachedId -ne $null) {
    return $cachedId
  }

  $query = New-Object Microsoft.Xrm.Sdk.Query.QueryExpression($EntityName)
  $query.TopCount = 1
  $query.NoLock = $True
  $query.ColumnSet = New-Object Microsoft.Xrm.Sdk.Query.ColumnSet($PrimaryKeyField, $SearchField)
  $query.Criteria.AddCondition($SearchField, [Microsoft.Xrm.Sdk.Query.ConditionOperator]::Equal, $SearchText)
  $records = ($CrmConn.RetrieveMultiple($query)).Entities;
  if ($records.Count -eq 0) {
    throw "Searching Primary Key (ID) value is failed for the ""$EntityName"" entity. Could not find a record with ""$SearchText"" value in the ""$SearchField"" field."
  }

  $record = $records | Select -First 1
  [Guid]$result = $record.Id
  $cachedRecords.Add($SearchText, $result)
  return $result
}

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
    [Char]$Delimeter
  )

  Write-Output "Exporting records to .CSV by the query from the $FetchXmlQueryFilePath file."
  Write-Verbose "Parsing FetchXml..."
  [System.Collections.ArrayList]$result = New-Object System.Collections.ArrayList
  $attrs = Get-FetchXmlQueryAttributes -CrmConn $CrmConn -FetchXmlQueryFilePath $FetchXmlQueryFilePath
  [string]$fetchXmlText = Get-Content $FetchXmlQueryFilePath -Raw
  $records = (Get-CrmRecordsByFetch -conn $CrmConn -Fetch $fetchXmlText -AllRows).CrmRecords
  Write-Output  "$($records.Count) records are being exported..."
  
  foreach($record in $records){
    $row = [pscustomobject]@{}
    foreach($attr in $attrs){
      $attrName = $attr.AttrFullName
      $valuePropName = $attrName + "_Property"
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
      
      Add-Member -InputObject $row -MemberType NoteProperty -Name $attrName -Value $value
      if ($attr.Type -in "Lookup", "Customer", "Owner", "Picklist", "State", "Status") { 
        $formattedValue = $value
        if ($formattedValue -ne $null) {
          $formattedValue = $record.$attrName
        }

        # in case it is a lookup aliased value, handle it approporiatly
        if ($formattedValue -is [Microsoft.Xrm.Sdk.EntityReference]) {
          $formattedValue = $formattedValue.Name
        }

        $nameColumnName = Get-EntityReferenceNameColumnName $attrName
        Add-Member -InputObject $row -MemberType NoteProperty -Name $nameColumnName -Value $formattedValue

        if ($attr.Type -in "Lookup", "Customer", "Owner") { 
          $entityTypeName = $value
          if ($entityTypeName -ne $null) {
            if ($entityTypeName -is [Microsoft.Xrm.Sdk.EntityReference]) {
              $entityTypeName = $formattedValue.LogicalName
            } else {
					    if($attPropValue -is [Microsoft.Xrm.Sdk.EntityReference]){
						    $entityTypeName = $attPropValue.LogicalName
					    } else {
                $entityTypeName = $null
              }
            }
          }

          $typeColumnName = Get-EntityReferenceTypeColumnName $attrName
          Add-Member -InputObject $row -MemberType NoteProperty -Name $typeColumnName -Value $entityTypeName
        }
      }
    }

    $result.Add($row) | Out-Null
  }
  
  if ($Delimeter -eq $null) {
    $result | Export-Csv -Path $ExportedFilePath -NoTypeInformation
  } else {
    $result | Export-Csv -Path $ExportedFilePath -NoTypeInformation -Delimiter $Delimeter
  }

  Write-Output "The data has been exported into $ExportedFilePath file."
}


function Get-FetchXmlQueryAttributes {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$FetchXmlQueryFilePath
  )

  Write-Verbose "Parsing FetchXml..."
  [xml]$fetchXml = Get-Content $FetchXmlQueryFilePath -Raw
  [System.Collections.ArrayList]$result = New-Object System.Collections.ArrayList
  [System.Xml.XmlNodeList]$attrs = $fetchXml.GetElementsByTagName('attribute')
  
  $rootEntityName = $fetchXml.GetElementsByTagName('entity')[0].Name
  $entityList = @($rootEntityName)
  $attrsByEntities = @()
	foreach($attr in $attrs){
    $parentNode = $attr.ParentNode;
    $parentNodeTagName = $parentNode.LocalName
    $name = $attr.name

    if ($parentNodeTagName -eq "link-entity") {
	    if($parentNode.HasAttribute('alias')){
		    $alias = $parentNode.GetAttribute('alias') 
	    } 
	    else {
		    $alias = $parentNode.GetAttribute('to')
	    }

	    if($attr.HasAttribute('alias')){
		    $attrName = $attr.GetAttribute('alias') 
	    } 
	    else {
		    $attrName = $attr.name
	    }

      $attrFullName = $alias  + "." + $attrName
      $entityName = $parentNode.GetAttribute('name')
      if (!$entityList.Contains($entityName)){
        $entityList += $entityList
      }
    }
	  else {
	    if($attr.HasAttribute('alias')){
		    $attrFullName = $attr.GetAttribute('alias') 
	    } 
	    else {
		    $attrFullName = $attr.name
	    }

      $entityName = $rootEntityName
	  }
    
    $attrsByEntities += [pscustomobject] @{ EntityName = $entityName; AttrName = $attr.name; AttrFullName = $attrFullName }
	}

  $attrsByEntities | Group-Object -Property EntityName | ForEach-Object {
    $entityName = $_.Name 
    $attrFields = $_.Group |  Select -ExpandProperty AttrName -Unique
    $entityAttrMetadata = Get-EntityAttributesInfo -CrmConn $conn -EntityName $entityName -AttributeNames $attrFields
    $attrsByEntities | Where-Object EntityName -EQ $entityName | ForEach-Object {
      Add-Member -InputObject $_ -MemberType NoteProperty -Name Type -Value $entityAttrMetadata.Attributes[$_.AttrName]
    }
  }

  return $attrsByEntities
}

function Get-EntitiesWhereUpsertDoesNotWork {
  return @("transactioncurrency")
}

function Import-EntityData {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$ImportFilePath,
    [Parameter(Mandatory=$True)]
    [String]$EntityName,
    [Parameter(Mandatory=$True)]
    [String[]]$AttributeNames,
    [Parameter(Mandatory=$False)]
    [hashtable]$MappingConfig
  )

  Write-Output "Importing ""$EntityName"" records. Source file: $ImportFilePath."
  if ($AttributeNames -ne $null){
    Write-Output "Attributes to import: $([String]::Join(",", $AttributeNames))"
  }

  $rows = Import-Csv -Path $ImportFilePath
  Write-Output "The source file has been read. $($rows.length) records are to import."
  $attributesToExclude = @("modifiedby", "modifiedon")
  $AttributeNames = $AttributeNames | Where-Object { $attributesToExclude -notcontains $_ }
  Write-Verbose "Getting entity attributes metadata..."
  $attrsInfo = Get-EntityAttributesInfo -CrmConn $CrmConn -EntityName $EntityName -AttributeNames $AttributeNames
  $attrs = $attrsInfo.Attributes
  $primaryIdAttribute = $attrsInfo.PrimaryIdAttribute

  Write-Verbose "Pre-processing importing data..."
  $rows = PreProcess-ImportingData -CrmConn $CrmConn -EntityName $EntityName -AttributesInfo $attrsInfo -Rows $rows -Verbose
  $impersonate = $AttributeNames.Contains("createdby")
  if ($impersonate){
    $currentUserId = $CrmConn.GetMyCrmUserId()
  }

  $useCreateRequest = $AttributeNames.Contains("createdon") -or $impersonate -eq $True
  if ($useCreateRequest) {
    Write-Warning "ATTENTION! 'Created On' or/and 'Created By' field(s) is going to be set manually (overridden). 'Create' request will be used instead of 'Upsert'."
  } else {
    $useCreateRequest = (Get-EntitiesWhereUpsertDoesNotWork) -contains $EntityName
    if ($useCreateRequest) {
      Write-Warning "ATTENTION! $EntityName entity does not support 'Upsert'. 'Create' request will be used instead of 'Upsert'. If the 'Create' action is fail due to duplication key issue, the system will use 'Update'."
    }
  }
  
  Write-Verbose "Importing..."
  $i = 1
  foreach ($row in $rows){
    [Microsoft.Xrm.Sdk.Entity]$record = New-Object Microsoft.Xrm.Sdk.Entity -ArgumentList($EntityName)
    $fields = @{}
    foreach($attr in $attrs.GetEnumerator()) {
      $attributeData = Build-EntityAttribute -EntityAttributeInfo $attr -Row $row
      $record.Attributes.Add($attributeData.AttrName, $attributeData.Value)
    }

    $recordId = $record.Attributes[$primaryIdAttribute] 
    $record.Id = $recordId
    Write-Verbose "Importing record number '$i'. (Id: '$recordId')"

    if ($useCreateRequest) {
      $request = New-Object -TypeName Microsoft.Xrm.Sdk.Messages.CreateRequest 
    } else {
      $request = New-Object -TypeName Microsoft.Xrm.Sdk.Messages.UpsertRequest
    }

    $request.Target = $record
    if ($impersonate) {
      $CrmConn.CallerId = $record.Attributes["createdby"].Id
    } else {
      $CrmConn.CallerId = [Guid]::Empty
    }

    try{

      $CrmConn.Execute($request) | Out-Null

    } catch [Exception] {
      Process-SpecialImportException -CrmConn $CrmConn -Exc $_.Exception -Record $record -Verbose
    }
    
    Write-Verbose "Record $recordId has been imported."
    $i = $i + 1
  }

  Write-Output "$($i-1) records have been imported into the ""$EntityName"" entity." 
}

function Process-SpecialImportException {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [Exception]$Exc,
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Sdk.Entity]$Record
  )

  [string]$errorMessage = $Exc.Message
  switch -Wildcard ($errorMessage){
    "Cannot insert duplicate*" {
      Write-Verbose "Record $recordId already exists. 'Update' method will be used instead of 'Create' for this record. Note: 'Created On' and 'Created By' fields will not update for this record."
      $request = New-Object -TypeName Microsoft.Xrm.Sdk.Messages.UpdateRequest
      $request.Target = $record
      try {
        $CrmConn.Execute($request) | Out-Null
      }
      catch [Exception] {
        Process-SpecialImportException -CrmConn $CrmConn -Exc $_.Exception -Record $record -Verbose
      }
    }

    "The exchange rate of the base currency cannot be modified*" {
      $request = New-Object -TypeName Microsoft.Xrm.Sdk.Messages.UpdateRequest
      Write-Verbose "'exchangerate' attribute has been removed from the attributes for the base currency record to update."
      $record.Attributes.Remove("exchangerate")
      $request.Target = $record
      $CrmConn.Execute($request) | Out-Null
    }
    default {
      throw $Exc
    }
  }

}

function Build-EntityAttribute {
  [CmdletBinding()]  
  param(
    [Parameter(Mandatory=$True)]
    [PSCustomObject]$EntityAttributeInfo, # a custom object containing info about attribute name and type
    [Parameter(Mandatory=$True)]
    [PSCustomObject]$Row # row of the .csv file
  )

  $attr = $EntityAttributeInfo
  [string]$attrName = $attr.Key
  $attrType = $attr.Value
  $columnValue = $Row.$attrName
  if ([string]::IsNullOrEmpty($columnValue)) {
    $value = $null 
  } else {
    switch ($attrType) {
      {$_ -in "String", "Memo" } { $value = $columnValue }
        
      "Boolean" { 
        $value = [Boolean]::Parse($columnValue) 
      }

      "DateTime" { 
        $value = [DateTime]::Parse($columnValue) 
        if ($attrName -eq "createdon") {
          $attrName = "overriddencreatedon"
        }
      }
        
      "Integer" { $value = [Int32]::Parse($columnValue) }

      { $_ -in "Lookup", "Customer", "Owner" } { 
        $typeColumnName = Get-EntityReferenceTypeColumnName $attrName
        $type = $row.$typeColumnName
        $useMapping = $MappingConfig -ne $null -and $MappingConfig.$attrName -ne $null -and $MappingConfig.$attrName.$type -ne $null
        if ($useMapping){
          $mapping = $MappingConfig.$attrName.$type
          $searchText = $row.$($mapping.SourceColumnName)
          $id = Get-IdValueFromCache -CrmConn $CrmConn -EntityName $type -PrimaryKeyField $mapping.PrimaryKeyField -SearchField $mapping.SearchField -SearchText $searchText
        } else {
          $id = [Guid]::Parse($columnValue)
        }

        $value = New-Object Microsoft.Xrm.Sdk.EntityReference -ArgumentList @($type, $id)
      }

      { $_ -in "Picklist", "State", "Status" } { 
        $value = New-Object Microsoft.Xrm.Sdk.OptionSetValue -ArgumentList @([Int32]::Parse($columnValue)) 
      }
        
      "Money" { $value = New-Object Microsoft.Xrm.Sdk.Money -ArgumentList @([decimal]::Parse($columnValue)) }
        
      "Decimal" { $value = [decimal]::Parse($columnValue) }
        
      "Double" { $value = [double]::Parse($columnValue) }
        
      "Uniqueidentifier" { $value = [Guid]::Parse($columnValue) }
        
      "EntityName" { $value = $columnValue }

      "BigInt" { $value = [Long]::Parse($columnValue) }

      "PartyList" {
        # TODO: implement PartyList support
        return $null
      }

      default { return $null }
    }
  }

  return [PSCustomObject]@{ AttrName = $attrName; Value = $value }
}

function PreProcess-ImportingData {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$EntityName,
    [Parameter(Mandatory=$True)]
    [PSCustomObject]$AttributesInfo,
    [Parameter(Mandatory=$True)]
    [PSCustomObject[]] $Rows
  )

  switch ($EntityName) {
    "businessunit" { 
      return PreProcess-BusinessUnitImportingData -CrmConn $CrmConn -Rows $Rows 
    }
    "transactioncurrency" {
      return PreProcess-TransactionCurrencyImportingData -CrmConn $CrmConn -Rows $Rows 
    }
    "team" {
      return PreProcess-TeamImportingData -CrmConn $CrmConn -Rows $Rows
    }
    #"queue" {}
    #"securityrole" {}
    #"systemuser" {}
    default { return $rows }
  }

  #$primaryIdAttribute = $attrsInfo.PrimaryIdAttribute
  #$rows | Where-Object {$_.$primaryIdAttribute }
}

function PreProcess-TransactionCurrencyImportingData {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [PSCustomObject[]] $Rows
  )

  Write-Verbose "PreProcess-TransactionCurrencyImportingData." 
  Write-Verbose "Finding base currency Id and ISO Code..."
  $fetchXmlText = "<fetch no-lock='true' >" + `
                    "<entity name='transactioncurrency' >" + `
                      "<attribute name='transactioncurrencyid' />" + `
                      "<attribute name='isocurrencycode' />" + `
                      "<filter>" + `
                        "<condition attribute='overriddencreatedon' operator='null' />" + `
                      "</filter>" + `
                      "<order attribute='createdon' />" + `
                    "</entity>" + `
                  "</fetch>"

  $targetBaseCurrency = (Get-CrmRecordsByFetch -conn $CrmConn -Fetch $fetchXmlText -TopCount 1).CrmRecords | Select-Object -First 1
  $targetBaseCurrencyIsoCode = $targetBaseCurrency.isocurrencycode

  Write-Verbose "Adjusting source base currency Id..."
  $Rows | Where-Object { $_.isocurrencycode -eq $targetBaseCurrencyIsoCode} | ForEach-Object {
    $_.transactioncurrencyid = $targetBaseCurrency.transactioncurrencyid
  }

  Write-Verbose "PreProcess-TransactionCurrencyImportingData is complete."
  return $Rows
}

function PreProcess-BusinessUnitImportingData {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [PSCustomObject[]] $Rows
  )

  Write-Verbose "PreProcess-BusinessUnitImportingData." 
  Write-Verbose "Finding root business unit Id..."
  $fetchXmlText = "<fetch no-lock='true' ><entity name='businessunit' ><attribute name='businessunitid' /><filter><condition attribute='parentbusinessunitid' operator='null' /></filter></entity></fetch>"
  $targetRootBusinessUnit = (Get-CrmRecordsByFetch -conn $CrmConn -Fetch $fetchXmlText -TopCount 1).CrmRecords | Select-Object -First 1

  Write-Verbose "Adjusting source root business unit Id..."
  $parentBusinessUnitIdFieldName = "parentbusinessunitid"
  $sourceRootBusinessUnitRow = $Rows | Where-Object {[String]::IsNullOrWhiteSpace($_.$parentBusinessUnitIdFieldName)} | Select-Object -First 1 
  $sourceRootBusinessUnitRow."businessunitid" = $targetRootBusinessUnit."businessunitid"
  $rootBusinessUnitName = $sourceRootBusinessUnitRow.name
  $result = @($sourceRootBusinessUnitRow)
  $result += Get-SortedChildBusinessUnits -Rows $Rows -BusinessUnitName $rootBusinessUnitName 
  Write-Verbose "PreProcess-BusinessUnitImportingData is complete."
  return $result
}

function PreProcess-TeamImportingData {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [PSCustomObject[]] $Rows
  )

  Write-Verbose "PreProcess-TeamImportingData." 
  Write-Verbose "Finding target default teams Id..."
  $fetchXmlText = "<fetch no-lock='true' >" + `
                    "<entity name='team' >" + `
                      "<attribute name='teamid' />" + `
                      "<attribute name='name' />" + `
                      "<filter>" + `
                        "<condition attribute='isdefault' operator='eq' value='1' />" + `
                      "</filter>" + `
                    "</entity>" + `
                  "</fetch>"
  $targetDefaultTeams = (Get-CrmRecordsByFetch -conn $CrmConn -Fetch $fetchXmlText).CrmRecords 
  Write-Verbose "Adjusting source default teams Id..."
  foreach ($team in $targetDefaultTeams) {
    $Rows | Where-Object { $_.isdefault -eq $True -and $_.name -eq $team.name} | ForEach-Object {
      Write-Verbose "$($_.teamid) -  $($team.teamid)"
      $_.teamid = $team.teamid
    }
  }


  #$Rows | Where-Object { $_.isdefault -eq $True} | ForEach-Object {
  #  $teamName = $_.name
  #   $id = $targetDefaultTeams | Where-Object $_.name -eq $teamName | Select-Object -First 1
  #   $_.teamid
  #}

  Write-Verbose "PreProcess-TeamCurrencyImportingData is complete."
  return $Rows
}

function Get-SortedChildBusinessUnits {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [PSCustomObject[]] $Rows,
    [Parameter(Mandatory=$True)]
    [String] $BusinessUnitName
  )
  
  Write-Verbose "Getting child business units for '$BusinessUnitName'..."
  $result = @()
  [PSCustomObject[]]$childRows = $Rows | Where-Object { $_."parentbusinessunitid!name" -eq $BusinessUnitName }
  $result = $childRows
  foreach($row in $childRows) {
    $result = $result + (Get-SortedChildBusinessUnits -Rows $Rows -BusinessUnitName $row."name")
  }

  return $result
}

function Get-EntityReferenceTypeColumnName {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [String]$ValueColumnName
  )

  return $ValueColumnName + "!type"
}

function Get-EntityReferenceNameColumnName {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [String]$ValueColumnName
  )

  return $ValueColumnName + "!name"
}

function Get-EntityAttributesInfo {
  [CmdletBinding()]
  param (
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$EntityName,
    [Parameter(Mandatory=$True)]
    [String[]]$AttributeNames
  )

  Write-Verbose "Quering metadata for the '$EntityName' entity..."

  $entityConditionExp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.MetadataConditionExpression 
  $entityConditionExp.PropertyName = "LogicalName"
  $entityConditionExp.ConditionOperator = [Microsoft.Xrm.Sdk.Metadata.Query.MetadataConditionOperator]::Equals
  $entityConditionExp.Value = $EntityName
  $entityFilterExp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.MetadataFilterExpression
  $entityFilterExp.Conditions.Add($entityConditionExp)
  $entityProp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.MetadataPropertiesExpression
  $entityProp.AllProperties = $false
  $entityProp.PropertyNames.AddRange("LogicalName", "Attributes", "PrimaryIdAttribute")

  
  $attrConditionExp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.MetadataConditionExpression 
  $attrConditionExp.PropertyName = "LogicalName"
  $attrConditionExp.ConditionOperator = [Microsoft.Xrm.Sdk.Metadata.Query.MetadataConditionOperator]::Equals
  $attrConditionExp.Value = "accountid"
  $attrConditionExp.ConditionOperator = [Microsoft.Xrm.Sdk.Metadata.Query.MetadataConditionOperator]::In
  $attrConditionExp.Value = $AttributeNames

  $attrFilterExp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.MetadataFilterExpression
  $attrFilterExp.Conditions.Add($attrConditionExp) | Out-Null
  $attrProp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.MetadataPropertiesExpression
  $attrProp.AllProperties = $false
  $attrProp.PropertyNames.AddRange("LogicalName", "AttributeType")

  $attrQueryExp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.AttributeQueryExpression
  $attrQueryExp.Criteria = $attrFilterExp
  $attrQueryExp.Properties = $attrProp
  
  $entityQueryExp = New-Object Microsoft.Xrm.Sdk.Metadata.Query.EntityQueryExpression
  $entityQueryExp.AttributeQuery = $attrQueryExp
  $entityQueryExp.Criteria = $entityFilterExp
  $entityQueryExp.Properties = $entityProp
  
  $request = New-Object Microsoft.Xrm.Sdk.Messages.RetrieveMetadataChangesRequest 
  $request.Query = $entityQueryExp
  
  $response = (Execute-CrmRequest $CrmConn $request) -as [Microsoft.Xrm.Sdk.Messages.RetrieveMetadataChangesResponse]
  $entityMetadata = ($response.EntityMetadata | Select-Object -First 1)
  if ($entityMetadata -eq $null){
    throw "An error occured! '$EntityName' entity is not found in the system" 
  }

  [hashtable] $attrs = @{}
  foreach ($attr in $entityMetadata.Attributes) {
    $attrs[$attr.LogicalName] = $attr.AttributeType
  }
  
  $result = @{ 
    PrimaryIdAttribute = $entityMetadata.PrimaryIdAttribute;
    Attributes = $attrs
  }


  return $result

}

function Execute-CrmRequest {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Sdk.OrganizationRequest]$request
  )

  $response = $CrmConn.ExecuteCrmOrganizationRequest($request, $null)
	if($response -eq $null){
	  throw $CrmConn.LastCrmException
	}

  return $response
}

# client scripts

function Get-DynamicsConnection {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [string]$OrgName
  )

  $invokationFolder = $PSScriptRoot

  ## ----------------------- UNCOMMENT THIS AS NEEDED TO SETUP SMOOTH CONNECTION -----------------------------
  ## Note: 
  ## You have to store a password hash in a file. You can use \OtherHelpers\CreateSecureStringFile.ps1 
  ## to build its secure content.

  Write-Verbose "Prepare credentials to connect to Dynamics smoothly (using saved credentials)..."

  $serverUrl = "http://test01"
  $orgName = $OrgName # only On-Premise will need it
  $userName = "test\administrator"

  $pathToCred = "$invokationFolder\testcred.txt"
  $pass = Get-Content $pathToCred | ConvertTo-SecureString
  $cred = New-Object -Typename System.Management.Automation.PSCredential -argumentlist $userName, $pass

  Write-Verbose "Connecting to Dynamics..."
  
  # Connecting to On-Premise (note: uncomment one of the two "$conn=" rows below)  
  return (Connect-CrmOnPremDiscovery -Credential $cred -ServerUrl $serverUrl -OrganizationName $orgName)
}

function ImportCrmData-BusinessUnit {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$ImportFilePath
  )

  $businessUnitMappingConfig = @{ 
    "parentbusinessunitid" = @{
      "businessunit" = @{
        SourceColumnName = "parentbusinessunitid!name";
        SearchField = "name";
        PrimaryKeyField = "businessunitid";
      }
    };
  }

  [string[]] $attrs = "businessunitid", "name", "parentbusinessunitid", "description", "isdisabled", "address1_line1", "address1_line2", `
                      "address1_line3", "address1_city", "address1_country", "address1_postalcode", "address1_telephone1", "address1_telephone2", `
                      "address2_line1", "address2_line2", "address2_line3", "address2_city", "address2_country", "address2_postalcode", `
                      "address2_telephone1", "address2_telephone2"

  Import-EntityData -CrmConn $CrmConn -ImportFilePath $ImportFilePath -EntityName "businessunit" `
    -AttributeNames $attrs -MappingConfig $businessUnitMappingConfig -Verbose 
}

function ImportCrmData-Currency {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$ImportFilePath
  )

  [string[]] $attrs = "transactioncurrencyid", "isocurrencycode", "currencyname", "currencysymbol", "currencyprecision", "exchangerate"
  Import-EntityData -CrmConn $CrmConn -ImportFilePath $ImportFilePath -EntityName "transactioncurrency" `
    -AttributeNames $attrs -Verbose 
}


function ImportCrmData-Team {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$True)]
    [Microsoft.Xrm.Tooling.Connector.CrmServiceClient]$CrmConn,
    [Parameter(Mandatory=$True)]
    [String]$ImportFilePath
  )


  $mappingConfig = @{ 
    "administratorid" = @{
      "systemuser" = @{
        SourceColumnName = "administratorid!name";
        SearchField = "fullname";
        PrimaryKeyField = "systemuserid";
      };
    };
    "businessunitid" = @{
      "businessunit" = @{
        SourceColumnName = "businessunitid!name";
        SearchField = "name";
        PrimaryKeyField = "businessunitid";
      };
    };
  }

  [string[]] $attrs = "teamid", "name" , "yominame", "businessunitid", "teamtype", "emailaddress", "administratorid", "description"
  Import-EntityData -CrmConn $CrmConn -ImportFilePath $ImportFilePath -EntityName "team" `
    -AttributeNames $attrs -MappingConfig $mappingConfig -Verbose 
}

try{
  Clear-Host 
  $error.clear()

  $invokationFolder = $PSScriptRoot
  $dataFolder = "$invokationFolder\Data"

  $sourceCrmConnection = Get-DynamicsConnection -OrgName org01
  write-output $hst.Name
  ##Import-Module ("$invokationFolder\CommonLib.ps1")

  ### ----------------------- UNCOMMENT THIS AS NEEDED TO SETUP SMOOTH CONNECTION -----------------------------
  ### Note: 
  ### You have to store a password hash in a file. You can use \OtherHelpers\CreateSecureStringFile.ps1 
  ### to build its secure content.

  #Write-Output "Prepare credentials to connect to Dynamics smoothly (using saved credentials)..."

  #$serverUrl = "http://test01"
  #$orgName = "org04" # only On-Premise will need it
  #$userName = "test\administrator"

  #$pathToCred = "$invokationFolder\testcred.txt"
  #$pass = Get-Content $pathToCred | ConvertTo-SecureString
  #$cred = new-object -typename System.Management.Automation.PSCredential -argumentlist $userName, $pass

  #Write-Output "Connecting to Dynamics..."

  ## Connecting to On-Premise (note: uncomment one of the two "$conn=" rows below)  
  #$conn = Connect-CrmOnPremDiscovery -Credential $cred -ServerUrl $serverUrl -OrganizationName $orgName 

  ## Connecting to On-Online (note: uncomment one of the two "$conn=" rows above/below)  
  #$conn = Connect-CrmOnline -Credential $cred -ServerUrl $serverUrl
  
  ## ---------------------------------------------------------------------------------------------------------
  
  # Connecting to a Dynamics instance/organization using "INTERACTIVE MODE" / WIZARD MODE
  # Note: Comment the row below in case you decided to uncomment the block above to connect smoothly   
  #$conn = Build-CrmConnection -InteractiveMode -Verbose

  #$fields = Get-FetchXmlQueryAttributes -CrmConn $conn -FetchXmlQueryFilePath "$invokationFolder\Account.xml" 
  #$fieldsNew = $fields.Clone()
  #$fields | Group-Object -Property EntityName | ForEach-Object {
  #  $entityName = $_.Name 
  #  $attrFields = $_.Group |  Select -ExpandProperty AttrName -Unique
  #  $entityAttrMetadata = Get-EntityAttributesInfo -CrmConn $conn -EntityName $entityName -AttributeNames $attrFields
  #  $fieldsNew | Where-Object EntityName -EQ $entityName | ForEach-Object {
  #    Add-Member -InputObject $_ -MemberType NoteProperty -Name Type -Value $entityAttrMetadata.Attributes[$_.AttrName]
  #  }

  #  Write-Output $entityAttrMetadata
  #}

  #Write-Output $fields

  #Export-EntityData -CrmConn $conn `
  #  -FetchXmlQueryFilePath "$invokationFolder\Contact.xml" `
  #  -ExportedFilePath "$invokationFolder\temp\contact.csv" 

  #Export-EntityData -CrmConn $conn `
  #  -FetchXmlQueryFilePath "$invokationFolder\Account.xml" `
  #  -ExportedFilePath "$invokationFolder\temp\account.csv" 

  ## Export Business Units
  #Export-EntityData -CrmConn $conn `
  #  -FetchXmlQueryFilePath "$invokationFolder\BusinessUnit.xml" `
  #  -ExportedFilePath "$invokationFolder\temp\BusinessUnit.csv" 

  # Export Transaction Currencies
  #Export-EntityData -CrmConn $conn `
  #  -FetchXmlQueryFilePath "$invokationFolder\TransactionCurrency.xml" `
  #  -ExportedFilePath "$dataFolder\TransactionCurrency.csv" 

  # Export Teams
  #Export-EntityData -CrmConn $conn `
  #  -FetchXmlQueryFilePath "$invokationFolder\Team.xml" `
  #  -ExportedFilePath "$dataFolder\Team.csv" 

  $sourceCrmConnection.Dispose() # reason: there is no way to set RequireNewInstance=True while creating a connection
  $destinationCrmConnection = Get-DynamicsConnection -OrgName org04
  
  #[string[]] $attrs = "accountid", "name", "primarycontactid", "openrevenue", "statecode", "statuscode"
  #[string[]] $attrs = "accountid", "name", "telephone1", "shippingmethodcode" #, "statecode" , "statuscode"
  #Import-EntityData -CrmConn $conn -ImportFilePath  "$invokationFolder\temp\test4.csv" -EntityName "account" -AttributeNames $attrs -Verbose

  $contactMappingConfig = @{ 
    "ownerid" = @{
      "systemuser" = @{
        SourceColumnName = "ownerid!name";
        SearchField = "fullname";
        PrimaryKeyField = "systemuserid";
      };
      "team" = @{
        SourceColumnName = "ownerid!name";
        SearchField = "name";
        PrimaryKeyField = "teamid";
      }
    }
  }

  ## [IMPORT] Contacts - Base
  #[string[]] $attrs = "contactid", "firstname", "lastname", "telephone1", "emailaddress1", "numberofchildren", "educationcode", "creditonhold", "birthdate", "aging30", "address1_line1", "address1_line2", "address1_stateorprovince", "address1_latitude", "address1_county", "address1_country", "address1_city", "preferredcontactmethodcode", "ownerid"
  #Import-EntityData -CrmConn $conn -ImportFilePath  "$invokationFolder\temp\contact.csv" -EntityName "contact" -AttributeNames $attrs -Verbose -MappingConfig $contactMappingConfig


  $accountMappingConfig = @{ 
    "createdby" = @{
      "systemuser" = @{
        SourceColumnName = "ownerid!name";
        SearchField = "fullname";
        PrimaryKeyField = "systemuserid";
      }
    };

    "ownerid" = @{
      "systemuser" = @{
        SourceColumnName = "ownerid!name";
        SearchField = "fullname";
        PrimaryKeyField = "systemuserid";
      };
      "team" = @{
        SourceColumnName = "ownerid!name";
        SearchField = "name";
        PrimaryKeyField = "teamid";
      }
    }
  } 
  
  # Accounts - Base
  #[string[]] $attrs = @("accountid", "name", "telephone1", "shippingmethodcode", "primarycontactid" , "createdon", "modifiedon", "modifiedby", "createdby", "ownerid")
  #Import-EntityData -CrmConn $conn -ImportFilePath  "$invokationFolder\temp\account.csv" -EntityName "account" -AttributeNames $attrs `
  #  -MappingConfig $accountMappingConfig -Verbose

  # Contacts - Parent Customer & State/Status
  #[string[]] $attrs = "contactid", "parentcustomerid", "statecode", "statuscode", "ownerid"
  #Import-EntityData -CrmConn $conn -ImportFilePath  "$invokationFolder\temp\contact.csv" -EntityName "contact" -AttributeNames $attrs -Verbose -MappingConfig $contactMappingConfig



  
  # [IMPORT] Business Units
  #ImportCrmData-BusinessUnit -CrmConn $destinationCrmConnection -ImportFilePath "$dataFolder\BusinessUnit.csv"

  #ImportCrmData-Currency -CrmConn $destinationCrmConnection -ImportFilePath "$dataFolder\TransactionCurrency.csv"
  ImportCrmData-Team -CrmConn $destinationCrmConnection -ImportFilePath "$dataFolder\Team.csv"
  $destinationCrmConnection.Dispose()
}

catch [Exception] {
  throw $_.Exception
}