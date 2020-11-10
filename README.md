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

Check if cluster is available and configured to give the tests a chance to run properly:
```
mvn test -Dcucumber.filter.tags="@DryRun"
```

Make the tests use AWS instead of CCM:

```
CLUSTER_KIND=aws mvn test -Dcucumber.filter.tags="@DryRun"
```
