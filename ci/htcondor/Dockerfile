FROM  htcondor/submit:el7 as submit

RUN yum install -y gcc git 
RUN yum install -y python3-devel python3-pip 
RUN pip3 install dask distributed pytest
RUN mkdir /root/secrets
VOLUME /root/secrets
RUN condor_store_cred -p password -f /root/secrets/pool_password 

FROM  htcondor/execute:el7 as execute 
RUN yum install -y python3
COPY --from=submit /usr/local/lib/python3.6 /usr/local/lib/python3.6
COPY --from=submit /usr/local/lib64/python3.6 /usr/local/lib64/python3.6
