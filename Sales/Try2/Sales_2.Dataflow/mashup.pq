[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared sales2 = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(central)", ItemKind = "Table"]}[Data],
  #"Appended query" = Table.Combine({#"Navigation 2", #"sample_sales(east)", #"sample_sales(south)", #"sample_sales(west)"}),
  #"Merged queries" = Table.NestedJoin(#"Appended query", {"OrderID"}, #"sample_sales(returns)", {"OrderID"}, "sample_sales(returns)", JoinKind.LeftOuter),
  #"Expanded sample_sales(returns)" = Table.ExpandTableColumn(#"Merged queries", "sample_sales(returns)", {"Returned"}, {"Returned"}),
  #"Inserted conditional column" = Table.AddColumn(#"Expanded sample_sales(returns)", "returned (2)", each if [Returned] = "Yes" then "Yes" else "No"),
  #"Removed columns" = Table.RemoveColumns(#"Inserted conditional column", {"Returned"}),
  #"Duplicated column" = Table.DuplicateColumn(#"Removed columns", "Sales", "Sales - Copy"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column", {{"Sales - Copy", Currency.Type}}),
  #"Replaced errors" = Table.ReplaceErrorValues(#"Changed column type", {{"Sales - Copy", 0}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced errors", {{"Discount", type number}, {"Quantity", type number}, {"Profit", Currency.Type}, {"returned (2)", type text}, {"OrderDate", type date}, {"ShipDate", type date}})

in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared #"sample_sales(east)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(east)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared #"sample_sales(returns)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(returns)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared #"sample_sales(salesperson)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(salesperson)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared #"sample_sales(south)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(south)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared #"sample_sales(west)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(west)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
