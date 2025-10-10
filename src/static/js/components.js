//src/js/components.js
// ============================================================================
// COMPONENT JAVASCRIPT
// ============================================================================

// File Upload Component
class FileUploadHandler {
    constructor(inputId, dropAreaSelector, fileInfoSelector) {
        this.input = document.getElementById(inputId);
        this.dropArea = document.querySelector(dropAreaSelector);
        this.fileInfo = document.querySelector(fileInfoSelector);
        this.fileName = this.fileInfo ? this.fileInfo.querySelector('#fileName, .file-name') : null;
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
                if (this.fileName) {
                    this.fileName.textContent = file.name;
                }
                if (this.fileInfo) {
                    this.fileInfo.style.display = 'block';
                }
                // Don't automatically enable submit button - let page logic handle it
                this.onFileSelected(file);
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
    
    onFileSelected(file) {
        // Hook for additional logic after file selection
        // Override in page-specific code if needed
    }
    
    reset() {
        if (this.fileInfo) {
            this.fileInfo.style.display = 'none';
        }
        if (this.submitBtn) {
            this.submitBtn.disabled = true;
        }
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
        
        // Check file size (16MB limit)
        const maxSize = 16 * 1024 * 1024; // 16MB in bytes
        if (file.size > maxSize) {
            alert('Le fichier dépasse la limite de 16 Mo. Veuillez choisir un fichier plus petit.');
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

// Multi-field Form Validator
class MultiFieldValidator {
    constructor(formSelector, requiredFields, submitButtonSelector) {
        this.form = document.querySelector(formSelector);
        this.requiredFields = requiredFields; // Array of field selectors
        this.submitButton = document.querySelector(submitButtonSelector);
        
        this.init();
    }
    
    init() {
        if (!this.form || !this.submitButton) return;
        
        // Add event listeners to all required fields
        this.requiredFields.forEach(fieldSelector => {
            const field = document.querySelector(fieldSelector);
            if (field) {
                field.addEventListener('change', () => this.validate());
                field.addEventListener('input', () => this.validate());
            }
        });
        
        // Initial validation
        this.validate();
    }
    
    validate() {
        let allValid = true;
        
        this.requiredFields.forEach(fieldSelector => {
            const field = document.querySelector(fieldSelector);
            if (!field) {
                allValid = false;
                return;
            }
            
            // Check based on field type
            if (field.type === 'file') {
                if (!field.files || field.files.length === 0) {
                    allValid = false;
                }
            } else if (field.tagName === 'SELECT') {
                if (!field.value || field.value === '') {
                    allValid = false;
                }
            } else {
                if (!field.value || field.value.trim() === '') {
                    allValid = false;
                }
            }
        });
        
        this.submitButton.disabled = !allValid;
        return allValid;
    }
}