[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared Returns = let
  Source = Csv.Document(Web.Contents("https://diacto-my.sharepoint.com/personal/nidhi_nair_diacto_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Returns.csv"), [Delimiter = ",", Columns = 2, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"Order ID", "OrderID"}})
in
  #"Renamed columns";
[BindToDefaultDestination = true]
shared Sales_Central = let
  Source = Csv.Document(Web.Contents("https://diacto-my.sharepoint.com/personal/nidhi_nair_diacto_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Sales_Central.csv"), [Delimiter = ",", Columns = 21, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Row ID", Int64.Type}, {"Order ID", type text}, {"Order Date", type date}, {"Ship Date", type date}, {"Ship Mode", type text}, {"Customer ID", type text}, {"Customer Name", type text}, {"Segment", type text}, {"Country", type text}, {"City", type text}, {"State", type text}, {"Postal Code", Int64.Type}, {"Region", type text}, {"Product ID", type text}, {"Category", type text}, {"Sub-Category", type text}, {"Product Name", type text}, {"Sales", Currency.Type}, {"Quantity", type number}, {"Discount", type number}, {"Profit", Currency.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Sales_East = let
  Source = Csv.Document(Web.Contents("https://diacto-my.sharepoint.com/personal/nidhi_nair_diacto_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Sales_East.csv"), [Delimiter = ",", Columns = 21, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Row ID", Int64.Type}, {"Order ID", type text}, {"Order Date", type date}, {"Ship Date", type date}, {"Ship Mode", type text}, {"Customer ID", type text}, {"Customer Name", type text}, {"Segment", type text}, {"Country", type text}, {"City", type text}, {"State", type text}, {"Postal Code", Int64.Type}, {"Region", type text}, {"Product ID", type text}, {"Category", type text}, {"Sub-Category", type text}, {"Product Name", type text}, {"Sales", type number}, {"Quantity", Int64.Type}, {"Discount", type number}, {"Profit", type number}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Row ID", "RowID"}})
in
  #"Renamed columns";
[BindToDefaultDestination = true]
shared Sales_South = let
  Source = Csv.Document(Web.Contents("https://diacto-my.sharepoint.com/personal/nidhi_nair_diacto_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Sales_South.csv"), [Delimiter = ",", Columns = 21, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Row ID", Int64.Type}, {"Order ID", type text}, {"Order Date", type date}, {"Ship Date", type date}, {"Ship Mode", type text}, {"Customer ID", type text}, {"Customer Name", type text}, {"Segment", type text}, {"Country", type text}, {"City", type text}, {"State", type text}, {"Postal Code", Int64.Type}, {"Region", type text}, {"Product ID", type text}, {"Category", type text}, {"Sub-Category", type text}, {"Product Name", type text}, {"Sales", type number}, {"Quantity", Int64.Type}, {"Discount", type number}, {"Profit", type number}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Sales_West = let
  Source = Csv.Document(Web.Contents("https://diacto-my.sharepoint.com/personal/nidhi_nair_diacto_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Sales_West.csv"), [Delimiter = ",", Columns = 21, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Row ID", Int64.Type}, {"Order ID", type text}, {"Order Date", type date}, {"Ship Date", type date}, {"Ship Mode", type text}, {"Customer ID", type text}, {"Customer Name", type text}, {"Segment", type text}, {"Country", type text}, {"City", type text}, {"State", type text}, {"Postal Code", Int64.Type}, {"Region", type text}, {"Product ID", type text}, {"Category", type text}, {"Sub-Category", type text}, {"Product Name", type text}, {"Sales", type number}, {"Quantity", Int64.Type}, {"Discount", type number}, {"Profit", type number}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared SalesPerson = let
  Source = Csv.Document(Web.Contents("https://diacto-my.sharepoint.com/personal/nidhi_nair_diacto_com/Documents/Apps/Microsoft Power Query/Uploaded Files/SalesPerson.csv"), [Delimiter = ",", Columns = 2, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
  #"Promoted headers";
[BindToDefaultDestination = true]
shared AllSales = let
  Source = Table.Combine({Sales_Central, Sales_East, Sales_South, Sales_West}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Sales", Currency.Type}, {"Profit", Currency.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"RowID"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Row ID", "RowID"}, {"Order ID", "OrderID"}, {"Order Date", "OrderDate"}, {"Ship Date", "ShipDate"}, {"Ship Mode", "ShipMode"}, {"Customer ID", "CustomerID"}, {"Customer Name", "CustomerName"}, {"Postal Code", "PostalCode"}, {"Product ID", "ProductID"}, {"Sub-Category", "SubCategory"}, {"Product Name", "ProductName"}})
in
  #"Renamed columns";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "740120ee-81a1-42e0-909b-aeb533ec57b3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "fc4134eb-7b14-41ed-9ca8-15245a7a9776"]}[Data]
in
  #"Navigation 2";
