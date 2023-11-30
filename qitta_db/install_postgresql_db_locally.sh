# reference: https://gist.github.com/gwangjinkim/f13bf596fefa7db7d31c22efd1627c7a


conda install -y -c conda-forge postgresql

####################################
# create a base database locally
####################################

initdb -D mylocal_db

##############################
# now start the server modus/instance of postgres
##############################

pg_ctl -D mylocal_db -l logfile start

## waiting for server to start.... done
## server started

# now the server is up


####################################
# create a non-superuser (more safety!)
####################################

createuser --encrypted --pwprompt mynonsuperuser
# asks for name and password

####################################
# using this super user, create inner database inside the base database
####################################

createdb --owner=mynonsuperuser myinner_db



################################
# stop running the postgres instance under ubuntu
################################

# monitor whether a postgres instance/server is running or not
ps aux | grep postgres
# if no instance is running, you will see only one line as the answer to your query - which is from your grep search!
# ending with: grep --color=auto postgres
# ignore this line!
# if an instance of postgresql server is running, then several
# processes are runnng
# you can kill the server by the first number of the leading line!

kill <number>

####################################
# run postgres as a non-server in the background
####################################

postgres -D db_djangogirls & # runs postgres
# press RET (return) to send it to background!

# you can stop and switch to server mode by
# following 'stop running postgres instance under ubuntu'

##############################
# stop non-server or server modus/instance of postgres
##############################

ps aux | grep postgres # see detailed instructions for finding the correct <process ID> 
# under 'stop running postgres instance under ubuntu'! And then do:
kill <process ID> # to stop postgres