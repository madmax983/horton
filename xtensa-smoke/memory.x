/* Linker script for the horton ESP32-S3 smoke test.
 *
 * The ROM bootloader loads our image segments from flash and jumps to
 * ENTRY(_start). All code/data lives in HP SRAM reached through the IRAM
 * bus (0x4037_0000 .. 0x403E_0000), which is executable. The stack is set up
 * manually in _start at the top of the DRAM view (0x3FCF_FFE0).
 *
 * Xtensa code loads constants with PC-relative `l32r`, whose pools live in
 * `.literal*` sections: every literal must be placed BEFORE the code that
 * loads it ("dangerous relocation: l32r: literal placed after use").
 */

ENTRY(_start);

MEMORY
{
    /* 448 KiB of HP SRAM via the IRAM bus (executable) */
    IRAM : ORIGIN = 0x40370000, LENGTH = 448K
}

SECTIONS
{
    .text : ALIGN(4)
    {
        *(.literal .literal.*);
        *(.text .text.*);
        *(.rodata .rodata.*);
    } > IRAM

    /* Zero-initialized data (RAM disk, Db, scratch). NOLOAD: the ROM does
       not zero it — _start zeroes it via _bss_start/_bss_end before any
       static is read. */
    .bss (NOLOAD) : ALIGN(4)
    {
        _bss_start = .;
        *(.bss .bss.*);
        *(COMMON);
        _bss_end = .;
    } > IRAM

    /DISCARD/ :
    {
        *(.comment .comment.*)
        *(.note .note.*)
    }
}

ASSERT(. <= 0x403E0000, "smoke image overflows IRAM");
