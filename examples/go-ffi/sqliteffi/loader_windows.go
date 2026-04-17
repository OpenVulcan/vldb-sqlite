//go:build windows

package sqliteffi

import "syscall"

// openLibrary 在 Windows 上加载 DLL。
// openLibrary loads a DLL on Windows.
func openLibrary(path string) (uintptr, error) {
	handle, err := syscall.LoadLibrary(path)
	if err != nil {
		return 0, err
	}
	return uintptr(handle), nil
}

// closeLibrary 在 Windows 上释放 DLL。
// closeLibrary releases a DLL on Windows.
func closeLibrary(handle uintptr) error {
	return syscall.FreeLibrary(syscall.Handle(handle))
}
