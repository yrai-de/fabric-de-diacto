[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared SupportCases = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "5f8b5e1c-90b8-4d75-ae29-8e0c904b8419"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "SupportCases"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Cases_7_29_2025.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 36, QuoteStyle = QuoteStyle.Csv]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Replaced value" = Table.ReplaceValue(#"Promoted headers", "", "Low", Replacer.ReplaceValue, {"Priority"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value", {{"(Do Not Modify) Case", type text}, {"(Do Not Modify) Row Checksum", type text}, {"(Do Not Modify) Modified On", type datetime}, {"Created On", type datetime}, {"Case Title", type text}, {"Case Number", type text}, {"Priority", type text}, {"Origin", type text}, {"Portfolio Company", type text}, {"Team", type text}, {"Support Rep", type text}, {"Status", type text}, {"Case Age", type text}, {"Case Status", type text}, {"Case Stage", type text}, {"Case Type", type text}, {"Closing Comments", type text}, {"Closing Reason", type text}, {"Contact", type text}, {"Deactivated On", type datetime}, {"Email Address", type text}, {"First Response By", type datetime}, {"First Response By KPI", type text}, {"First Response Sent", type text}, {"First Response SLA Status", type text}, {"Modified On", type datetime}, {"Modified By (Delegate)", type text}, {"Modified By", type text}, {"Modified By Username", type text}, {"Resolution", type text}, {"Resolution Date", type text}, {"Resolve By KPI", type text}, {"Resolve By SLA Status", type text}, {"SLA", type text}, {"Severity", type text}, {"Subject", type text}}),
  #"Replaced value 1" = Table.ReplaceValue(#"Changed column type", "", "Unspecified", Replacer.ReplaceValue, {"Team"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "", "Unspecified", Replacer.ReplaceValue, {"Support Rep"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "", "Unspecified", Replacer.ReplaceValue, {"Contact"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "", "Sev C (Non-Critical)", Replacer.ReplaceValue, {"Severity"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value 4", {{"Case Age", Int64.Type}}),
  #"Added custom" = Table.AddColumn(#"Changed column type 1", "Case Age by Day", each Number.RoundDown([Case Age] / 1440)),
  #"Renamed columns" = Table.RenameColumns(#"Added custom", {{"Created On", "CreatedOn"}, {"Case Title", "CaseTitle"}, {"Case Number", "CaseNumber"}, {"Portfolio Company", "PortfolioCompany"}, {"Support Rep", "SupportRep"}, {"Case Age", "CaseAge"}, {"Case Status", "CaseStatus"}, {"Case Stage", "CaseStage"}, {"Case Type", "CaseType"}, {"Closing Comments", "ClosingComments"}, {"Closing Reason", "ClosingReason"}, {"Deactivated On", "DeactivatedOn"}, {"Email Address", "EmailAddress"}, {"First Response By", "FirstResponseBy"}, {"First Response By KPI", "FirstResponseByKPI"}, {"First Response Sent", "FirstResponseSent"}, {"First Response SLA Status", "FirstResponseSLAStatus"}, {"Modified On", "ModifiedOn"}, {"Modified By (Delegate)", "ModifiedBy(Delegate)"}, {"Modified By", "ModifiedBy"}, {"Modified By Username", "ModifiedByUsername"}, {"Resolution Date", "ResolutionDate"}, {"Resolve By KPI", "ResolveByKPI"}, {"Resolve By SLA Status", "ResolveBySLAStatus"}, {"Case Age by Day", "CaseAgeByDay"}}),
  #"Added custom 1" = Table.AddColumn(#"Renamed columns", "TTR", each (Duration.Days([DeactivatedOn] - [CreatedOn]))),
  #"Renamed columns 1" = Table.RenameColumns(#"Added custom 1", {{"(Do Not Modify) Case", "DoNotModifyCase"}, {"(Do Not Modify) Row Checksum", "DoNotModifyRowChecksum"}, {"(Do Not Modify) Modified On", "DoNotModifyModifiedOn"}, {"ModifiedBy(Delegate)", "ModifiedByDelegate"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 1", {{"CaseAgeByDay", type text}}),
  #"Replaced errors" = Table.ReplaceErrorValues(#"Changed column type 2", {{"CaseAge", 0}}),
  #"Replaced errors 1" = Table.ReplaceErrorValues(#"Replaced errors", {{"CaseAgeByDay", "0"}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Replaced errors 1", {{"CaseAgeByDay", Int64.Type}, {"TTR", Int64.Type}}),
  #"Inserted quarter" = Table.AddColumn(#"Changed column type 3", "Quarter", each Date.QuarterOfYear([CreatedOn]), type nullable number),
  #"Inserted month" = Table.AddColumn(#"Inserted quarter", "Month", each Date.Month([CreatedOn]), type nullable number),
  #"Inserted month name" = Table.AddColumn(#"Inserted month", "Month name", each Date.MonthName([CreatedOn]), type nullable text),
  #"Inserted day" = Table.AddColumn(#"Inserted month name", "Day", each Date.Day([CreatedOn]), type nullable number),
  #"Added prefix" = Table.TransformColumns(#"Inserted day", {{"Quarter", each "Q" & Text.From(_), type text}}),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Added prefix", "Case Age Bucket", each if [CaseAgeByDay] <= 7 then "Under 7 days"
else if [CaseAgeByDay] <= 14 then "8-14 days"
else if [CaseAgeByDay] <= 30 then "15-30 days"
else if [CaseAgeByDay] <= 60 then "31-60 days"
else "Over 61 days"), {{"Case Age Bucket", type text}})
in
  #"Added custom 2";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "5f8b5e1c-90b8-4d75-ae29-8e0c904b8419"]}[Data]
in
  #"Navigation 2";
