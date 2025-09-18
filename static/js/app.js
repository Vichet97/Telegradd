/**
 * Telegradd Web Interface - Main JavaScript
 * Handles real-time updates, interactive features, and user interface enhancements
 */

// Global variables
let refreshInterval;
let notificationPermission = false;

// Initialize application when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

/**
 * Initialize the application
 */
function initializeApp() {
    // Request notification permission
    requestNotificationPermission();
    
    // Initialize tooltips
    initializeTooltips();
    
    // Initialize auto-refresh for dashboard
    if (window.location.pathname === '/dashboard') {
        startAutoRefresh();
    }
    
    // Initialize form validation
    initializeFormValidation();
    
    // Initialize keyboard shortcuts
    initializeKeyboardShortcuts();
    
    // Initialize theme toggle
    initializeThemeToggle();
    
    console.log('Telegradd Web Interface initialized successfully');
}

/**
 * Request notification permission from user
 */
function requestNotificationPermission() {
    if ('Notification' in window && Notification.permission === 'default') {
        Notification.requestPermission().then(function(permission) {
            notificationPermission = permission === 'granted';
        });
    } else if (Notification.permission === 'granted') {
        notificationPermission = true;
    }
}

/**
 * Show browser notification
 */
function showNotification(title, message, type = 'info') {
    if (!notificationPermission) return;
    
    const icon = type === 'success' ? '✅' : type === 'error' ? '❌' : 'ℹ️';
    
    new Notification(title, {
        body: message,
        icon: '/static/favicon.ico',
        tag: 'telegradd-notification'
    });
}

/**
 * Initialize Bootstrap tooltips
 */
function initializeTooltips() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

/**
 * Start auto-refresh for dashboard data
 */
function startAutoRefresh() {
    // Initial load
    refreshDashboardData();
    
    // Set up interval for auto-refresh every 30 seconds
    refreshInterval = setInterval(refreshDashboardData, 30000);
    
    // Clear interval when page is unloaded
    window.addEventListener('beforeunload', function() {
        if (refreshInterval) {
            clearInterval(refreshInterval);
        }
    });
}

/**
 * Refresh dashboard data via AJAX
 */
function refreshDashboardData() {
    const refreshIcon = document.getElementById('refreshIcon');
    if (refreshIcon) {
        refreshIcon.classList.add('refresh-btn');
    }
    
    // Refresh account status
    fetch('/api/accounts/status')
        .then(response => response.json())
        .then(data => {
            updateAccountsTable(data.accounts);
            updateStatistics(data);
            updateLastUpdated();
        })
        .catch(error => {
            console.error('Error refreshing account data:', error);
            showAlert('Error refreshing account data', 'error');
        });
    
    // Refresh background tasks
    fetch('/api/background-tasks')
        .then(response => response.json())
        .then(data => {
            updateBackgroundTasks(data.tasks);
            updateTaskStatistics(data.tasks);
        })
        .catch(error => {
            console.error('Error refreshing task data:', error);
        });
    
    // Remove refresh animation after 2 seconds
    setTimeout(() => {
        if (refreshIcon) {
            refreshIcon.classList.remove('refresh-btn');
        }
    }, 2000);
}

/**
 * Update accounts table with new data
 */
function updateAccountsTable(accounts) {
    const tbody = document.querySelector('#accountsTable tbody');
    if (!tbody) return;
    
    tbody.innerHTML = '';
    
    if (accounts.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-muted">
                    <i class="fas fa-inbox fa-2x mb-2"></i>
                    <br>No accounts found. Please add some accounts first.
                </td>
            </tr>
        `;
        return;
    }
    
    accounts.forEach(account => {
        const statusClass = getStatusClass(account.status);
        const row = createAccountRow(account, statusClass);
        tbody.innerHTML += row;
    });
}

/**
 * Get CSS class for account status
 */
function getStatusClass(status) {
    switch (status) {
        case 'online': return 'success';
        case 'offline': return 'danger';
        default: return 'warning';
    }
}

/**
 * Create HTML row for account
 */
function createAccountRow(account, statusClass) {
    return `
        <tr class="fade-in">
            <td><strong>${escapeHtml(account.phone)}</strong></td>
            <td>
                <span class="status-indicator status-${account.status}"></span>
                <span class="badge bg-${statusClass}">${capitalizeFirst(account.status)}</span>
            </td>
            <td><span class="badge bg-secondary">${account.type.toUpperCase()}</span></td>
            <td>${account.added_today}</td>
            <td>${account.remaining_limit}</td>
            <td><small class="text-muted">${escapeHtml(account.last_active)}</small></td>
        </tr>
    `;
}

/**
 * Update statistics cards
 */
function updateStatistics(data) {
    updateElement('totalAccounts', data.total);
    updateElement('onlineAccounts', data.online);
}

/**
 * Update task statistics
 */
function updateTaskStatistics(tasks) {
    updateElement('backgroundTasks', tasks.length);
}

/**
 * Update last updated timestamp
 */
function updateLastUpdated() {
    updateElement('lastUpdated', new Date().toLocaleTimeString());
}

/**
 * Update background tasks list
 */
function updateBackgroundTasks(tasks) {
    const container = document.getElementById('backgroundTasksList');
    if (!container) return;
    
    if (tasks.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted">
                <i class="fas fa-sleep fa-2x mb-2"></i>
                <br>No active tasks
            </div>
        `;
        return;
    }
    
    container.innerHTML = '';
    tasks.forEach(task => {
        const statusClass = task.status === 'running' ? 'success' : 'secondary';
        const taskHtml = `
            <div class="d-flex justify-content-between align-items-center mb-2 fade-in">
                <div>
                    <small class="fw-bold">${escapeHtml(task.name)}</small>
                    <br>
                    <small class="text-muted">${escapeHtml(task.status)}</small>
                </div>
                <span class="badge bg-${statusClass}">${escapeHtml(task.status)}</span>
            </div>
        `;
        container.innerHTML += taskHtml;
    });
}

/**
 * Initialize form validation
 */
function initializeFormValidation() {
    const forms = document.querySelectorAll('.needs-validation');
    
    Array.from(forms).forEach(form => {
        form.addEventListener('submit', function(event) {
            if (!form.checkValidity()) {
                event.preventDefault();
                event.stopPropagation();
            }
            form.classList.add('was-validated');
        });
    });
}

/**
 * Initialize keyboard shortcuts
 */
function initializeKeyboardShortcuts() {
    document.addEventListener('keydown', function(event) {
        // Ctrl/Cmd + R: Refresh data
        if ((event.ctrlKey || event.metaKey) && event.key === 'r') {
            event.preventDefault();
            if (typeof refreshDashboardData === 'function') {
                refreshDashboardData();
            }
        }
        
        // Escape: Close modals
        if (event.key === 'Escape') {
            const modals = document.querySelectorAll('.modal.show');
            modals.forEach(modal => {
                const bsModal = bootstrap.Modal.getInstance(modal);
                if (bsModal) bsModal.hide();
            });
        }
    });
}

/**
 * Initialize theme toggle functionality
 */
function initializeThemeToggle() {
    const themeToggle = document.getElementById('themeToggle');
    if (!themeToggle) return;
    
    // Load saved theme
    const savedTheme = localStorage.getItem('theme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);
    
    themeToggle.addEventListener('click', function() {
        const currentTheme = document.documentElement.getAttribute('data-theme');
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
        
        document.documentElement.setAttribute('data-theme', newTheme);
        localStorage.setItem('theme', newTheme);
    });
}

/**
 * Show alert message
 */
function showAlert(message, type = 'info', duration = 5000) {
    const alertContainer = document.getElementById('alertContainer') || createAlertContainer();
    
    const alertId = 'alert-' + Date.now();
    const alertClass = type === 'error' ? 'danger' : type;
    const iconClass = type === 'error' ? 'exclamation-triangle' : 
                     type === 'success' ? 'check-circle' : 'info-circle';
    
    const alertHtml = `
        <div id="${alertId}" class="alert alert-${alertClass} alert-dismissible fade show" role="alert">
            <i class="fas fa-${iconClass} me-2"></i>
            ${escapeHtml(message)}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    alertContainer.insertAdjacentHTML('beforeend', alertHtml);
    
    // Auto-dismiss after duration
    setTimeout(() => {
        const alert = document.getElementById(alertId);
        if (alert) {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }
    }, duration);
}

/**
 * Create alert container if it doesn't exist
 */
function createAlertContainer() {
    const container = document.createElement('div');
    container.id = 'alertContainer';
    container.className = 'position-fixed top-0 end-0 p-3';
    container.style.zIndex = '9999';
    document.body.appendChild(container);
    return container;
}

/**
 * Utility function to update element content
 */
function updateElement(id, content) {
    const element = document.getElementById(id);
    if (element) {
        element.textContent = content;
    }
}

/**
 * Utility function to escape HTML
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Utility function to capitalize first letter
 */
function capitalizeFirst(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Handle AJAX form submissions
 */
function submitForm(formId, successCallback, errorCallback) {
    const form = document.getElementById(formId);
    if (!form) return;
    
    const formData = new FormData(form);
    const submitButton = form.querySelector('button[type="submit"]');
    
    // Disable submit button and show loading state
    if (submitButton) {
        submitButton.disabled = true;
        submitButton.innerHTML = '<span class="spinner me-2"></span>Processing...';
    }
    
    fetch(form.action, {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            if (successCallback) successCallback(data);
            showAlert(data.message || 'Operation completed successfully', 'success');
        } else {
            if (errorCallback) errorCallback(data);
            showAlert(data.message || 'An error occurred', 'error');
        }
    })
    .catch(error => {
        console.error('Form submission error:', error);
        if (errorCallback) errorCallback(error);
        showAlert('Network error occurred', 'error');
    })
    .finally(() => {
        // Re-enable submit button
        if (submitButton) {
            submitButton.disabled = false;
            submitButton.innerHTML = submitButton.getAttribute('data-original-text') || 'Submit';
        }
    });
}

/**
 * Copy text to clipboard
 */
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showAlert('Copied to clipboard', 'success', 2000);
    }).catch(err => {
        console.error('Failed to copy text: ', err);
        showAlert('Failed to copy to clipboard', 'error');
    });
}

/**
 * Format file size
 */
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * Format duration
 */
function formatDuration(seconds) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    if (hours > 0) {
        return `${hours}h ${minutes}m ${secs}s`;
    } else if (minutes > 0) {
        return `${minutes}m ${secs}s`;
    } else {
        return `${secs}s`;
    }
}

/**
 * Debounce function for search inputs
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Export functions for global use
window.TelegraddApp = {
    refreshDashboardData,
    showAlert,
    submitForm,
    copyToClipboard,
    formatFileSize,
    formatDuration,
    debounce
};