#!/bin/sh

# Broker Options: important!
# The local-uri setting places the unix domain socket in rundir 
#   if FLUX_URI is not set, tools know where to connect.
#   -Slog-stderr-level= can be set to 7 for larger debug level
#   or exposed as a variable
brokerOptions="-Scron.directory=/etc/flux/system/cron.d \
  -Stbon.fanout=256 \
  -Srundir=/run/flux \
  -Sstatedir=${STATE_DIRECTORY:-/var/lib/flux} \
  -Slocal-uri=local:///run/flux/local \
  -Slog-stderr-level=6 \
  -Slog-stderr-mode=local"

# Derive hostname (this is a hack to get the one defined by the docker-compose network)
address=$(echo $( nslookup "$( hostname -i )" | head -n 1 ))
parts=(${address//=/ })
hostName=${parts[2]}
thisHost=(${hostName//./ })
thisHost=${thisHost[0]}
echo $thisHost

# Export this hostname
export FLUX_FAKE_HOSTNAME=$thisHost

cd ${workdir}
printf "\n👋 Hello, I'm ${thisHost}\n"
printf "The main host is ${mainHost}\n\n"
printf "🔍️ Here is what I found in the working directory, ${workdir}\n"
ls ${workdir}

# --cores=IDS Assign cores with IDS to each rank in R, so we  assign 1-N to 0
printf "\n📦 Resources\n"
sudo cat /etc/flux/system/R

printf "\n🦊 Independent Minister of Privilege\n"
cat /etc/flux/imp/conf.d/imp.toml

# The curve cert is generated on container build
# We assume the munge.key is the same also since we use the same base container!
# located at /etc/munge/munge.key

# Give broker time to start before workers
if [ ${thisHost} != "${mainHost}" ]; then
    printf "\n😪 Sleeping to give broker time to start...\n"
    sleep 15
    FLUX_FAKE_HOSTNAME=$thisHost flux start -o --config /etc/flux/config ${brokerOptions} sleep inf
else
    echo "Extra arguments are: $@"
    printf "flux start -o --config /etc/flux/config ${brokerOptions} sleep inf\n"
    FLUX_FAKE_HOSTNAME=$thisHost flux start -o --config /etc/flux/config ${brokerOptions} sleep inf
fi
