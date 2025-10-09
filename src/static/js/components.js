// ============================================================================
// COMPONENT JAVASCRIPT
// ============================================================================

// File Upload Component
class FileUploadHandler {
    constructor(inputId, dropAreaSelector, fileInfoSelector) {
        this.input = document.getElementById(inputId);
        this.dropArea = document.querySelector(dropAreaSelector);
        this.fileInfo = document.querySelector(fileInfoSelector);
        this.fileName = document.querySelector('#fileName');
        this.submitBtn = document.querySelector('button[type="submit"]');
        
        this.init();
    }
    
    init() {
        if (!this.input || !this.dropArea) return;
        
        this.input.addEventListener('change', () => this.handleFileSelect());
        
        // Drag and drop functionality
        this.dropArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            this.dropArea.classList.add('dragover');
        });

        this.dropArea.addEventListener('dragleave', () => {
            this.dropArea.classList.remove('dragover');
        });

        this.dropArea.addEventListener('drop', (e) => {
            e.preventDefault();
            this.dropArea.classList.remove('dragover');
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                this.input.files = files;
                this.handleFileSelect();
            }
        });
    }
    
    handleFileSelect() {
        const file = this.input.files[0];
        if (file) {
            if (this.validateFile(file)) {
                this.fileName.textContent = file.name;
                this.fileInfo.style.display = 'block';
                if (this.submitBtn) this.submitBtn.disabled = false;
            } else {
                this.reset();
            }
        } else {
            this.reset();
        }
    }
    
    validateFile(file) {
        // Override in subclasses for specific file type validation
        return true;
    }
    
    reset() {
        this.fileInfo.style.display = 'none';
        if (this.submitBtn) this.submitBtn.disabled = true;
        this.input.value = '';
    }
}

// PDF File Upload Handler
class PDFUploadHandler extends FileUploadHandler {
    validateFile(file) {
        if (!file.name.toLowerCase().endsWith('.pdf')) {
            alert('Veuillez sélectionner un fichier PDF');
            return false;
        }
        return true;
    }
}

// Excel File Upload Handler
class ExcelUploadHandler extends FileUploadHandler {
    validateFile(file) {
        const name = file.name.toLowerCase();
        if (!name.endsWith('.xls') && !name.endsWith('.xlsx')) {
            alert('Veuillez sélectionner un fichier Excel (.xls ou .xlsx)');
            return false;
        }
        return true;
    }
}

// Form Loading State Manager
class FormLoadingManager {
    constructor(formSelector, loadingSelector, resultsSelector) {
        this.form = document.querySelector(formSelector);
        this.loading = document.querySelector(loadingSelector);
        this.results = document.querySelector(resultsSelector);
        
        this.init();
    }
    
    init() {
        if (this.form) {
            this.form.addEventListener('submit', () => {
                if (this.loading) this.loading.style.display = 'block';
                if (this.results) this.results.style.display = 'none';
            });
        }
    }
}