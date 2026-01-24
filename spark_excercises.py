from pyspark.shell import sc

teamsAndGoals = sc.parallelize([("barcelona", 2), ("real madrid", 4), ("barcelona", 1), ("manchester utd", 2), ("bayern", 2), ("manchester utd", 3)])
