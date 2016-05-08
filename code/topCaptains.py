from collections import namedtuple
from pyspark import SparkContext

fields = ("name", "country", "career", "matches", "won", "lost", "ties", "toss" )
Captain = namedtuple( 'Captain', fields )

def parseRecs( line ):
    fields = line.split(",")
    return Captain( fields[0], fields[1], fields[2], int( fields[3] ),
                   int( fields[4] ), int(fields[5]), int(fields[6]), int(fields[7] ) )

def main( sc ):
    captains_odis = sc.textFile( "file:///home/hadoop/lab/data/captains_ODI.csv" )
    captains = captains_odis.map( lambda rec: parseRecs( rec) )
    captains_100 = captains.filter( lambda rec: rec.matches > 100 )
    captains_100_percent_wins = captains_100.map( lambda rec: ( rec.name, round( rec.won/rec.matches, 2 ) ) )
    captains_100_percent_wins.sortBy( lambda rec: rec[1], ascending = False ).collect()

    captains_tests = sc.textFile( "file:///home/hadoop/lab/data/captains_Test.csv" )
    captains_tests_recs = captains_tests.map( lambda rec: parseRecs( rec ) )
    captains_tests_50 = captains_tests_recs.filter( lambda rec: rec.matches > 50 )
    captain_top = captains_tests_50.map( lambda rec:
                                        ( rec.name, round( rec.won/rec.matches, 2 ) ) ).sortBy( lambda rec:
                                                                                      rec[1], ascending = False )
    all_time_best_captains = captains_100_percent_wins.join( captain_top )
    best_captains = all_time_best_captains.map( lambda rec: ( rec[0], rec[1][0], rec[1][1] ) )
    best_captains.repartition( 1 ).saveAsTextFile( "file:///home/hadoop/lab/results/topCaptains" )

if __name__ == "__main__":
    sc   = SparkContext()
    # Execute Main functionality
    main(sc)
