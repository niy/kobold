# Device Setup

To synchronize a Kobo eReader with Kobold, the device's configuration file must be modified to point to the Kobold server.

## Configuration Steps

1.  **Connect Device**: Connect the Kobo eReader to the computer via USB and tap **Connect** on the device.
2.  **Locate Config**: Open the mounted Kobo drive and navigate to `.kobo/Kobo/Kobo eReader.conf`.
    *   *Note*: The `.kobo` folder is hidden. Ensure hidden files are visible in the file explorer. On Mac, press `CMD + Shift + .` to show hidden files. On Windows, enable "Show hidden files, folders, and drives" in File Explorer settings.
3.  **Edit Config**: Open `Kobo eReader.conf` in a text editor.
4.  **Update Endpoint**: Locate the `[OneStoreServices]` section. Modify or add the `api_endpoint` key:

    ```ini
    [OneStoreServices]
    api_endpoint=http://<SERVER_IP>:<PORT>/api/kobo/<KB_USER_TOKEN>
    ```

    *Example*:
    ```ini
    [OneStoreServices]
    api_endpoint=http://192.168.1.50:8000/api/kobo/my-secret-token
    ```

    *Example with reverse proxy*:
    ```ini
    [OneStoreServices]
    api_endpoint=https://kobold.example.com/api/kobo/my-secret-token

5.  **Save and Eject**: Save the file and safely eject the device.
6.  **Sync**: Tap the **Sync** icon on the Kobo device. New books should now appear.

## Reverse Proxy (Nginx)

When running behind Nginx, ensure the following configuration is present in the `location` block to handle Kobo Sync Protocol headers:

```nginx
location / {
    proxy_pass http://localhost:8000;

    # Headers required for sync
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Port $server_port;

    # Buffer settings for large headers
    proxy_buffer_size 128k;
    proxy_buffers 4 256k;
    proxy_busy_buffers_size 256k;
    large_client_header_buffers 8 32k;
}
```
