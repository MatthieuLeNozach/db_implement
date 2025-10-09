// ============================================================================
// BASE JAVASCRIPT FUNCTIONALITY
// ============================================================================

// Theme Management
function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
}

// Load saved theme
function initializeTheme() {
    const savedTheme = localStorage.getItem('theme') || 'auto';
    document.documentElement.setAttribute('data-theme', savedTheme);
    const themeSelect = document.querySelector('.theme-toggle select');
    if (themeSelect) {
        themeSelect.value = savedTheme;
    }
}

// Copy to clipboard utility
function copyToClipboard(text, button) {
    navigator.clipboard.writeText(text).then(() => {
        const originalText = button.textContent;
        button.textContent = 'Copié!';
        button.classList.add('copied');
        setTimeout(() => {
            button.textContent = originalText;
            button.classList.remove('copied');
        }, 2000);
    });
}

// Initialize theme on DOM load
document.addEventListener('DOMContentLoaded', initializeTheme);