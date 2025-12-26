(() => {
    'use strict';

    const generateToken = () => {
        const input = document.querySelector('.ks-token-input');
        if (!input) return;

        const array = new Uint8Array(16);
        crypto.getRandomValues(array);
        const token = [...array].map(b => b.toString(16).padStart(2, '0')).join('');

        input.value = token;
        updateDockerCompose(token);
    };

    const updateDockerCompose = (token) => {
        const searchPattern = 'KB_USER_TOKEN=__YOUR_TOKEN_HERE__';
        document.querySelectorAll('code').forEach(code => {
            if (code.textContent.includes(searchPattern)) {
                code.innerHTML = code.innerHTML.replace(searchPattern, 'KB_USER_TOKEN=' + token);
            }
        });
    };

    const copyToken = (input) => {
        if (!input.value) return;
        input.select();
        navigator.clipboard.writeText(input.value);
    };

    if (typeof document$ !== 'undefined') {
        document$.subscribe(generateToken);
    } else {
        document.addEventListener('DOMContentLoaded', generateToken);
    }

    document.body.addEventListener('click', (e) => {
        if (e.target.closest('.ks-generate-btn')) {
            e.preventDefault();
            generateToken();
        }

        const input = e.target.closest('.ks-token-input');
        if (input) {
            copyToken(input);
        }
    });
})();
