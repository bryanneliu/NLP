#!/bin/bash

./match.sh | tee treatment

echo '******'
echo '* Change against baseline'
echo '* < baseline'
echo '* > treatment'
echo '******'
diff baseline treatment

