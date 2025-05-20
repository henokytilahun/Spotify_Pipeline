from fastapi import FastAPI, Request

app = FastAPI()

@app.get("/callback")
async def callback(request: Request):
    code = request.query_params.get("code")
    if code:
        print(f"Received Spotify auth code: {code}")
        return {"message": "Authorization code received. You can close this window."}
    else:
        return {"error": "No code in query params"}
