# RUN THIS IN POWERSHELL IF YOU NEED THE TOKEN
```$body=@{grant_type='client_credentials';client_id='denis9365-api-client';client_secret='yNcZDA7L6DKM47pvGe6yqlLZDed8R708'}; $res=Invoke-RestMethod -Uri "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token" -Method Post -ContentType "application/x-www-form-urlencoded" -Body $body; $res.access_token```

# How to run:
 1. pip install -r requirements.txt
 2. Pray.
 