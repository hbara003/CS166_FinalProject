
# CS166 Final Project
## How to Install and Run
download onto Linux machine with postgreSQL

      git clone https://github.com/hbara003/CS166_FinalProject.git

cd into /postgresql and start then create the database 

      . ./startPostgreSQL.sh
      . ./createPostgreDB.sh

cd into /java and compile then run:

      . ./compile.sh
      . ./run.sh $LOGNAME'_DB' 9998 $LOGNAME
