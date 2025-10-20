# Walrus Sui Archival Website

This is a static website that displays information from the Walrus Sui Archival system. It fetches data from the REST API and renders it client-side.

## Structure

```
website/
├── index.html          # Main HTML page
├── css/
│   └── style.css       # Stylesheet
├── js/
│   ├── config.js       # Network configuration
│   └── main.js         # Main JavaScript logic
└── README.md           # This file
```

## Configuration

To switch between testnet and mainnet, edit `js/config.js`:

```javascript
const CONFIG = {
    network: 'testnet', // Change to 'mainnet' for mainnet
    // ...
};
```

### Available Networks

- **testnet**: `https://walrus-sui-archival.testnet.walrus.space`
- **mainnet**: `https://walrus-sui-archival.mainnet.walrus.space`

## Deployment

### Option 1: Serve Locally

Use any static file server. For example, with Python:

```bash
# Python 3
python3 -m http.server 8000

# Python 2
python -m SimpleHTTPServer 8000
```

Then open http://localhost:8000 in your browser.

### Option 2: Serve with Node.js

```bash
npx http-server .
```

### Option 3: Deploy to Static Hosting

This website can be deployed to any static hosting service:

- **GitHub Pages**: Push to a `gh-pages` branch
- **Netlify**: Drag and drop the `website/` folder
- **Vercel**: Deploy via CLI or web interface
- **AWS S3**: Upload to an S3 bucket with static website hosting enabled
- **Cloudflare Pages**: Connect your repository

## Features

- **Dynamic Data Loading**: Fetches live data from the archival API
- **Network Switching**: Easy configuration to switch between testnet/mainnet
- **Responsive Design**: Works on desktop and mobile devices
- **Error Handling**: Displays friendly error messages if data cannot be loaded
- **Loading States**: Shows loading spinner while fetching data

## API Endpoint

The website fetches data from:
- `/v1/app_info_for_homepage` - Returns JSON with homepage statistics

### Example Response

```json
{
  "blob_count": 42,
  "total_checkpoints": 12345,
  "earliest_checkpoint": 1000,
  "latest_checkpoint": 13345,
  "total_size": 5368709120,
  "metadata_info": {
    "metadata_pointer_object_id": "0x123...",
    "contract_package_id": "0x456...",
    "current_metadata_blob_id": "AkV8qvSaD9n9Vjo97sZGGXD8QD1eDC8KNbdRZhQ5aqPc"
  }
}
```

## Development

### Testing Locally

1. Edit `js/config.js` to point to your local server:
   ```javascript
   endpoints: {
       testnet: 'http://localhost:8080',
       // ...
   }
   ```

2. Start the archival REST API server

3. Serve the website locally (see deployment options above)

### Browser Compatibility

This website uses modern JavaScript features:
- Fetch API
- Async/await
- ES6 syntax

Supported browsers:
- Chrome 55+
- Firefox 52+
- Safari 10.1+
- Edge 15+

## CORS Considerations

If you encounter CORS errors when fetching data, ensure the REST API server has CORS enabled for the domain serving this website.

For local development, you may need to:
1. Use a CORS browser extension
2. Configure the REST API server to allow CORS from `localhost`
3. Use a reverse proxy

## License

Copyright (c) Mysten Labs, Inc.
SPDX-License-Identifier: Apache-2.0
