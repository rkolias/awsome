## AWSome

A companion library for the AWS Java SDK that provides helpful functionality to simplify integration with AWS services.

#### Dependencies

- AWS Java SDK
- Guava

#### Integration with Maven

        <dependency>
            <groupId>io.awsome</groupId>
            <artifactId>awsome</artifactId>
            <version>${awsome.version}</version>
        </dependency>

### Examples

#### Cloudsearch

AWSome provides a `StructuredQueryBuilder` to help build structured search queries.

The various query builder functions may be imported statically for less verbose code:

	import static io.awsome.cloudsearch.StructuredQueryBuilder.and;
	import static io.awsome.cloudsearch.StructuredQueryBuilder.eq;

Structured queries are built using one or more of the functions provided:

	String query = and(eq("title", "star wars")).build();		
	SearchRequest searchRequest = new SearchRequest().withQueryParser(QueryParser.Structured).withQuery(query);
