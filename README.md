## AWSome

A companion library for the AWS Java SDK that provides helpful functionality to simplify integration with AWS services.

#### Dependencies

- AWS Java SDK
- Guava
- Jackson

#### Integration with Maven

        <dependency>
            <groupId>io.awsome</groupId>
            <artifactId>awsome</artifactId>
            <version>${awsome.version}</version>
        </dependency>

### Examples

#### Cloudsearch

##### StructuredQueryBuilder

The `StructuredQueryBuilder` helps build structured search queries.

The various query builder functions may be imported statically for less verbose code:

	import static io.awsome.cloudsearch.StructuredQueryBuilder.and;
	import static io.awsome.cloudsearch.StructuredQueryBuilder.eq;

Structured queries are built using one or more of the functions provided:

	String query = and(eq("title", "star wars")).build();		
	SearchRequest searchRequest = new SearchRequest().withQueryParser(QueryParser.Structured).withQuery(query);

##### UploadDocumentsBuilder

The `UploadDocumentBuilder` helps add and delete documents from Cloudsearch.

Define a class representing your Cloudsearch schema and add Jackson annotations for JSON serialization:

	@JsonSerialize
	public class MyDocument {
		@JsonProperty(value="field1")
		private String field1;
		...
	}

Create a `UploadDocumentBuilder` and add and remove documents:

	UploadDocumentsBuilder<MyDocument> builder = new UploadDocumentsBuilder<>();

	MyDocument document = new MyDocument("value1", ...);

	builder.add("id.1", document);
	builder.delete("id.2");

Obtain a `UploadDocumentsRequest` with the `build()` method:

	UploadDocumentsRequest request = builder.build();

