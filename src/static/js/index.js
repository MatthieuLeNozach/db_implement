// src/js/pages/index.js
// ============================================================================
// INDEX PAGE JAVASCRIPT
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Initialize PDF File Upload Handler with custom validation
    const pdfHandler = new PDFUploadHandler('pdfInput', '.drag-drop-area', '#fileInfo');
    
    // Override handleFileSelect to include customer validation
    const originalHandleFileSelect = pdfHandler.handleFileSelect.bind(pdfHandler);
    pdfHandler.handleFileSelect = function() {
        originalHandleFileSelect();
        updateUploadButton();
    };
    
    // Customer select handler
    const customerSelect = document.getElementById('customerSelect');
    if (customerSelect) {
        customerSelect.addEventListener('change', updateUploadButton);
    }
    
    // Function to enable/disable upload button based on both file and customer selection
    function updateUploadButton() {
        const submitBtn = document.querySelector('#uploadBtn');
        const fileInput = document.getElementById('pdfInput');
        const customerSelect = document.getElementById('customerSelect');
        
        if (submitBtn && fileInput && customerSelect) {
            const hasFile = fileInput.files && fileInput.files.length > 0;
            const hasCustomer = customerSelect.value !== '';
            submitBtn.disabled = !(hasFile && hasCustomer);
        }
    }
    
    // Initialize Form Loading Manager
    new FormLoadingManager('#uploadForm', '#loading', '#results');
    
    // Copy table data function (specific to index page)
    window.copyTableData = function(button) {
        const table = document.getElementById('cleanDataTable');
        if (!table) {
            console.error('Table not found');
            alert('Table not found');
            return;
        }
        
        const rows = table.querySelectorAll('tbody tr');
        let tableData = '';
        
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            const rowData = Array.from(cells).map(cell => cell.textContent.trim()).join('\t');
            tableData += rowData + '\n';
        });
        
        if (tableData.trim() === '') {
            console.error('No data to copy');
            alert('No data to copy');
            return;
        }
        
        navigator.clipboard.writeText(tableData.trim()).then(() => {
            const originalText = button.textContent;
            button.textContent = '✅ Copied!';
            button.classList.add('copied');
            
            setTimeout(() => {
                button.textContent = originalText;
                button.classList.remove('copied');
            }, 2000);
        }).catch((err) => {
            console.error('Failed to copy table data: ', err);
            alert('Failed to copy table data to clipboard. Error: ' + err.message);
        });
    };
});