
FROM python:3.10-slim
WORKDIR /usr/src/app

COPY .streamlit .
COPY requirements.txt .
COPY functions.py .
COPY app.py .
COPY logo_taxi.jpg .
COPY ./aggbaselicense .
COPY ./aggreportbanbases .
COPY ./aggreportbancases .
COPY ./aggreportbanvehicles .
COPY ./aggreportshlinspectschedule .
COPY ./aggreportuniquevehicle .
COPY ./aqe .
COPY ./availablevehiclesblack .
COPY ./availablevehiclesgreen .
COPY ./availablevehicleshighvolume .
COPY ./availablevehicleslimo .
COPY ./availablevehicleslivery .
COPY ./availablevehiclesyellow .
COPY ./fareboxpertripsblack .
COPY ./fareboxpertripsgreen .
COPY ./fareboxpertripshighvolume .
COPY ./fareboxpertripslimo .
COPY ./fareboxpertripslivery .
COPY ./fareboxpertripsyellow .
COPY ./fhvbasetypecontained .
COPY ./fhvpopulardropofflocation .
COPY ./fhvpopularpickuplocation .
COPY ./fhvtotaltripspermonth .
COPY ./fhvtotaltripsperyear .
COPY ./fhvtriptimemoving .
COPY ./fhvtriptimeoperating .
COPY ./fhvvehicletype .
COPY ./greendistancemoving .
COPY ./greengroupadditionalmoney .
COPY ./greenpopulardropofflocation .
COPY ./greenpopularpickuplocation .
COPY ./greenunpaidmoney .
COPY ./hourlymonitoring .
COPY ./nynta .
COPY ./reportedbase .
COPY ./reportedvehicles .
COPY ./shlmetershops .
COPY ./taxizonegeo .
COPY ./taxizonelookup .
COPY ./tlcnewdriverapplicationstat .
COPY ./veh .
COPY ./versionofvehicles .
COPY ./yellowdistancemoving .
COPY ./yellowgroupadditionalmoney .
COPY ./yellowpopulardropofflocation .
COPY ./yellowpopularpickuplocation .
COPY ./yellowunpaidmoney .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]

