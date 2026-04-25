from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from data import generate_uber_ride_confirmation
from producer import send_events

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
def booking_home(request: Request):
    return templates.TemplateResponse(request=request, name="home.html")

@app.get("/book")
def book_ride(request: Request):
    
    uber_data = generate_uber_ride_confirmation()
    print("data generated sucessfully")
    send_events(uber_data)
    return templates.TemplateResponse(request=request, name="confirmation.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)