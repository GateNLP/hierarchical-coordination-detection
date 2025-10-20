# Co-ordination service React UI

This folder contains the UI for the coordination service, implemented with React.  The main entry point is `index.html`, which in turn loads `src/index.jsx`.  The app is built using [Vite](https://vitejs.dev).

## Running in development mode

For development, the React app is served by the Vite dev server at http://localhost:5173.  The dev server acts as a reverse proxy, passing API calls through to the Python flask app on `http://127.0.0.1:5000` - if your flask app is running on a different port you may need to edit the proxy configuration in `vite.config.js`.

## Building for production

In production, the compiled versions of the UI artifacts are served by the Flask app from its `static` folder.  The top-level `Dockerfile` takes care of running the production Vite build and placing the output artifacts in the right place in the final image.

There is also the option to build separate front and back end images, either because you want to plug the existing back end into a different front end UI, or because you want to put the front and back ends separately behind the same reverse proxy.  To build these separate images, pass `--target backend` or `--target frontend` to the `docker buildx` command.