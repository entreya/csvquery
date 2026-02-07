---
name: Compatibility Report
about: Report installation or compatibility issues with the daemon/binary
title: "[Compat] OS Name - PHP Version"
labels: compatibility
assignees: ''

---

**System Information**
- **OS**: [e.g. Windows 11, Ubuntu 22.04, macOS Sonoma]
- **Architecture**: [e.g. x64, arm64]
- **PHP Version**: `php -v` [e.g. 8.2.14]
- **Composer Version**: `composer --version`

**Checklist**
Please run the following checks:
- [ ] Binary exists in `bin/` folder
- [ ] `php scripts/build.php` runs successfully (if applicable)
- [ ] Daemon starts manually (`./bin/csvquery... daemon`)
- [ ] PHP Socket Connection works

**Error Log**
If you have an error output, please paste it here:
```
Paste error here
```

**Additional Context**
Add any other context about the problem here.
