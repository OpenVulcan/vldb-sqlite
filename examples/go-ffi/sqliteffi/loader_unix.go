//go:build linux || darwin || freebsd

package sqliteffi

import "github.com/ebitengine/purego"

// openLibrary 在 Unix 系统上打开动态库。
// openLibrary opens a dynamic library on Unix-like systems.
func openLibrary(path string) (uintptr, error) {
	return purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_LOCAL)
}

// closeLibrary 在 Unix 系统上关闭动态库。
// closeLibrary closes a dynamic library on Unix-like systems.
func closeLibrary(handle uintptr) error {
	return purego.Dlclose(handle)
}
