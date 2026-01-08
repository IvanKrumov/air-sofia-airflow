import airbase

client = airbase.AirbaseClient()

request = client.request(
    "Verified",
    "BG",
    poll=["PM2.5", "PM10"]
)

print("Request created successfully!")
print("Downloading to ./data/bulgaria_verified/")

# Correct method name
request.download(
    dir="./data/bulgaria_verified",
    skip_existing=True
)

print("✓ Download complete!")