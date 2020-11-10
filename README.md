# cassandra-rtest
Integration tests for Repairs

Runs with: 
```
mvn test
```

Or per-feature:
```
mvn test -Dcucumber.filter.tags="@Full"
mvn test -Dcucumber.filter.tags="@Incremental"
mvn test -Dcucumber.filter.tags="@Subrange"
```
