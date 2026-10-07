# Product Hunter Portal

This is the new responsive portal for Product Hunter. It is designed for mobile Safari and desktop use, with demo mode enabled by default.

## Use immediately

Open `portal/index.html` in a browser. It provides demo data, filters, score explanations, hunt transparency, local save, and CSV export without requiring credentials.

## Connect the backend

Open the browser console and run:

```js
localStorage.setItem('ph_api_base', 'https://YOUR-RENDER-SERVICE.onrender.com')
location.reload()
```

The portal then attempts the existing endpoint:

```text
GET /auto-hunt/{category}
```

The frontend accepts `products` or `results` arrays from the response. For production, the backend should return normalized records with `title`, `category`, `source`, `price`, `rating`, `reviews`, `score`, and `accepted` fields.

## Recommended Vercel setup

Set the Vercel project root to the repository root and use `portal/index.html` as the portal entry point, or copy this file to the deployed site's public entry after reviewing the existing frontend. Do not place API keys in this HTML file.
