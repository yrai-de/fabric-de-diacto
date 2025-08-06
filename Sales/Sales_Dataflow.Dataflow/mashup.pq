[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared All_Sales = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(central)", ItemKind = "Table"]}[Data],
  #"Appended query" = Table.Combine({#"Navigation 2", #"sample_sales(east)", #"sample_sales(south)", #"sample_sales(west)"}),
  #"Merged queries" = Table.NestedJoin(#"Appended query", {"OrderID"}, #"sample_sales(returns)", {"OrderID"}, "sample_sales(returns)", JoinKind.LeftOuter),
  #"Expanded sample_sales(returns)" = Table.ExpandTableColumn(#"Merged queries", "sample_sales(returns)", {"Returned", "OrderID"}, {"Returned", "OrderID.1"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded sample_sales(returns)", {"Returned", "OrderID.1"}),
  #"Merged queries 1" = Table.NestedJoin(#"Removed columns", {"OrderID"}, #"sample_sales(returns)", {"OrderID"}, "sample_sales(returns)", JoinKind.LeftOuter),
  #"Expanded sample_sales(returns) 1" = Table.ExpandTableColumn(#"Merged queries 1", "sample_sales(returns)", {"Returned", "OrderID"}, {"sample_sales(returns).Returned", "sample_sales(returns).OrderID"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Expanded sample_sales(returns) 1", {"sample_sales(returns).Returned", "sample_sales(returns).OrderID"}),
  #"Merged queries 2" = Table.NestedJoin(#"Removed columns 1", {"OrderID"}, #"sample_sales(returns)", {"OrderID"}, "sample_sales(returns)", JoinKind.LeftOuter),
  #"Expanded sample_sales(returns) 2" = Table.ExpandTableColumn(#"Merged queries 2", "sample_sales(returns)", {"Returned"}, {"Returned"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded sample_sales(returns) 2", {{"Sales", type number}, {"ShipDate", type date}, {"Quantity", type number}, {"Discount", type number}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type", "If Returned", each if [Returned] = "Yes" then "Yes" else "No"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted conditional column", {{"If Returned", type text}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Changed column type 1", {"Returned"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns 2", {{"Sales", type text}, {"Profit", Currency.Type}, {"OrderDate", type date}})
in
  #"Changed column type 2";
shared #"sample_sales(east)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(east)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
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
shared #"sample_sales(south)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(south)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared #"sample_sales(west)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sample_sales(west)", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "e2af9891-1525-4030-adda-61a676f46f85"]}[Data]
in
  #"Navigation 2";
